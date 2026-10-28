"""Python 3.10+, discord.py 2.x. Run ONE process per SQLite database."""
import asyncio
import io
import json
import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands, tasks

GUILD_ID = int(os.environ['GUILD_ID'])
CATEGORY_ID = int(os.environ['TICKET_CATEGORY_ID'])
STAFF_IDS = {int(x.strip()) for x in os.environ['STAFF_ROLE_IDS'].split(',') if x.strip()}
LOG_ID = int(os.getenv('TICKET_LOG_CHANNEL_ID', '0'))
DB_PATH = os.getenv('TICKET_DB_PATH', 'tickets.sqlite3')
MAX_OPEN = int(os.getenv('MAX_OPEN_TICKETS', '3'))
REMIND_MINUTES = int(os.getenv('TICKET_REMIND_MINUTES', '60'))
COOLDOWN_SECONDS = int(os.getenv('TICKET_COOLDOWN_SECONDS', '60'))
RETENTION_DAYS = int(os.getenv('TICKET_RETENTION_DAYS', '30'))
if COOLDOWN_SECONDS < 0 or RETENTION_DAYS < 0:
    raise RuntimeError('Cooldown and retention must be >= 0')
if REMIND_MINUTES < 0:
    raise RuntimeError('TICKET_REMIND_MINUTES must be >= 0')
STATE_LABELS = {'waiting': '未対応', 'active': '対応中', 'archived': '完了・保管中', 'updating': '権限更新の確認待ち'}
if not STAFF_IDS or MAX_OPEN < 1:
    raise RuntimeError('STAFF_ROLE_IDS and MAX_OPEN_TICKETS must be valid')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('ticketbot')


@dataclass(frozen=True)
class Desk:
    label: str
    roles: tuple[int, ...]
    urgent: bool = False


PRODUCTION_DESKS = {
    'executive': Desk('経営・重要事項（執行部）', (1562091893091733624, 1562093163567386724)),
    'staff': Desk('スタッフ・人員整理（組織運営・管理部門）', (1562093397907611728, 1562092379568087222)),
    'permissions': Desk('Discordの権限（組織運営・管理部門）', (1562092379568087222,)),
    'development': Desk('開発・サービス（開発担当）', (1562093559916527668,)),
    'singer': Desk('歌い手活動（担当）', (1562705998026772521,)),
    'management': Desk('マネジメント業務', (1562705811002626108,)),
    'publicity': Desk('SNS・広報（広報部門）', (1562094485062680696,)),
    'office': Desk('書類・事務手続き（事務部門）', (1562094639270461541,)),
    'support': Desk('利用者からの問い合わせ（サポート部門）', (1562094372311277648,)),
    'report': Desk('【第22条】ハラスメント・規則違反の相談（緊急通報）', (1562091893091733624,), True),
}

# Test IDs are explicitly supplied; never look up private routing by role name.
TEST_ROLE_IDS = {
    'executive': (1564583710572019734, 1564583710572019733),
    'staff': (1564583710572019732, 1564583710563766300),
    'permissions': (1564583710572019732,),
    'development': (1564583710563766299,),
    'singer': (1564583710550921256,),
    'management': (1564583710550921257,),
    'publicity': (1564583710550921260,),
    'office': (1564583710550921259,),
    'support': (1564583710550921261,),
    'report': (1564583710572019734,),
}
TICKET_ENV = os.getenv('TICKET_ENV', 'production').strip().lower()
if TICKET_ENV not in ('test', 'production'):
    raise RuntimeError('TICKET_ENV must be test or production')
DESKS = (PRODUCTION_DESKS if TICKET_ENV == 'production' else {
    key: Desk(desk.label, TEST_ROLE_IDS[key], desk.urgent)
    for key, desk in PRODUCTION_DESKS.items()
})


def parse_member_id(value: str) -> int | None:
    value = value.strip()
    if not value:
        return None
    match = re.fullmatch(r'(?:<@!?(\d{15,22})>|(\d{15,22}))', value)
    if not match:
        raise ValueError('当事者は1人のユーザーID、またはメンションで入力してください。')
    return int(match.group(1) or match.group(2))


def staff(member: discord.Member) -> bool:
    return not member.bot and any(r.id in STAFF_IDS for r in member.roles)


class TicketError(Exception):
    pass


class Store:
    def __init__(self):
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(DB_PATH)
        self.db.execute('CREATE TABLE IF NOT EXISTS tickets (channel INTEGER PRIMARY KEY, owner INTEGER NOT NULL, desk TEXT NOT NULL, excluded INTEGER)')
        # Idempotent migration of v0.1.x databases. No existing ticket is deleted.
        columns = {r[1] for r in self.db.execute('PRAGMA table_info(tickets)')}
        for name, definition in {
            'status': "TEXT NOT NULL DEFAULT 'waiting'",
            'assignee': 'INTEGER', 'created_at': 'REAL NOT NULL DEFAULT 0',
            'notified_at': 'REAL NOT NULL DEFAULT 0',
            'revision': 'INTEGER NOT NULL DEFAULT 0',
            'sensitive': 'INTEGER NOT NULL DEFAULT 0', 'pending': 'TEXT',
            'notes_channel': 'INTEGER', 'archived_at': 'REAL NOT NULL DEFAULT 0',
            'retention_notified': 'REAL NOT NULL DEFAULT 0',
        }.items():
            if name not in columns:
                self.db.execute(f'ALTER TABLE tickets ADD COLUMN {name} {definition}')
        self.db.execute('CREATE TABLE IF NOT EXISTS ticket_audit (id INTEGER PRIMARY KEY, ticket INTEGER NOT NULL, at REAL NOT NULL, actor INTEGER, event TEXT NOT NULL, desk TEXT, subject_id INTEGER)')
        self.db.execute('CREATE TABLE IF NOT EXISTS creation_cooldowns (owner INTEGER PRIMARY KEY, at REAL NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS orphan_notes (channel INTEGER PRIMARY KEY, ticket INTEGER NOT NULL)')
        self.db.execute("UPDATE tickets SET archived_at=? WHERE status='archived' AND archived_at=0", (time.time(),))
        self.db.execute('UPDATE tickets SET created_at=? WHERE created_at=0', (time.time(),))
        self.db.execute("UPDATE tickets SET sensitive=1 WHERE desk='report'")
        self.db.commit()

    def info(self, cid):
        self.db.row_factory = sqlite3.Row
        row = self.db.execute('SELECT * FROM tickets WHERE channel=?', (cid,)).fetchone()
        self.db.row_factory = None
        return dict(row) if row else None

    def update(self, cid, *, actor=None, event=None, subject_id=None, **fields):
        allowed = {'status', 'assignee', 'created_at', 'notified_at', 'sensitive', 'desk', 'pending',
                   'notes_channel', 'archived_at', 'retention_notified'}
        if not fields or not set(fields) <= allowed:
            raise ValueError('Invalid ticket update')
        sql = ','.join(f'{key}=?' for key in fields)
        self.db.execute(f'UPDATE tickets SET {sql},revision=revision+1 WHERE channel=?', (*fields.values(), cid))
        if event:
            self._audit(cid, actor, event, fields.get('desk'), subject_id)
        self.db.commit()

    def _audit(self, cid, actor, event, desk=None, subject_id=None):
        # Metadata only; no free-form payload, message, title, or note text.
        events = {'created', 'claimed', 'released', 'assigned', 'transition_requested',
                  'transition_applied', 'transfer_proposed', 'transfer_rejected',
                  'notes_created', 'notes_opened', 'delete_requested', 'deleted', 'archived', 'reopened', 'transferred',
                  'channel_removed', 'retention_notice', 'notes_deleted'}
        if event not in events or (desk is not None and desk not in DESKS):
            raise ValueError('Invalid audit metadata')
        self.db.execute('INSERT INTO ticket_audit (ticket,at,actor,event,desk,subject_id) VALUES (?,?,?,?,?,?)',
                        (cid, time.time(), actor, event, desk, subject_id))

    def audit(self, cid, actor, event, desk=None, subject_id=None):
        self._audit(cid, actor, event, desk, subject_id)
        self.db.commit()

    def cooldown_remaining(self, owner):
        row = self.db.execute('SELECT at FROM creation_cooldowns WHERE owner=?', (owner,)).fetchone()
        return max(0, int(row[0] + COOLDOWN_SECONDS - time.time()) + 1) if row and COOLDOWN_SECONDS else 0

    def created_now(self, owner):
        self.db.execute('INSERT OR REPLACE INTO creation_cooldowns VALUES (?,?)', (owner, time.time()))
        self.db.commit()

    def get(self, cid):
        return self.db.execute('SELECT owner,desk,excluded FROM tickets WHERE channel=?', (cid,)).fetchone()

    def add(self, cid, owner, desk, excluded):
        self.db.execute('INSERT INTO tickets (channel,owner,desk,excluded,created_at,sensitive) VALUES (?,?,?,?,?,?)',
                        (cid, owner, desk, excluded, time.time(), int(DESKS[desk].urgent)))
        self.db.commit()

    def remove(self, cid):
        info = self.info(cid)
        if info and info['notes_channel']:
            self.db.execute('INSERT OR IGNORE INTO orphan_notes VALUES (?,?)', (info['notes_channel'], cid))
        self.db.execute('DELETE FROM tickets WHERE channel=?', (cid,))
        self.db.commit()


async def reply(interaction, text):
    if interaction.response.is_done():
        await interaction.followup.send(text, ephemeral=True)
    else:
        await interaction.response.send_message(text, ephemeral=True)


class TicketBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.guilds = intents.members = intents.message_content = True
        super().__init__(command_prefix=commands.when_mentioned, intents=intents,
                         allowed_mentions=discord.AllowedMentions.none())
        self.store = Store()
        self.create_lock = asyncio.Lock()
        self.close_lock = asyncio.Lock()

    async def setup_hook(self):
        self.add_view(PanelView())
        self.add_view(CloseView())
        self.add_view(LegacyControls())
        await self.tree.sync(guild=discord.Object(id=GUILD_ID))
        self.reminders.start()

    async def close(self):
        self.reminders.cancel()
        task = self.reminders.get_task()
        if task:
            await asyncio.gather(task, return_exceptions=True)
        await super().close()
        self.store.db.close()

    async def on_guild_channel_delete(self, channel):
        if channel.guild.id == GUILD_ID:
            if self.store.info(channel.id):
                self.store.audit(channel.id, None, 'channel_removed')
                self.store.remove(channel.id)
            self.store.db.execute('UPDATE tickets SET notes_channel=NULL WHERE notes_channel=?', (channel.id,))
            self.store.db.commit()

    async def on_guild_channel_update(self, before, after):
        if after.guild.id != GUILD_ID or before.name == after.name:
            return
        async with self.close_lock:
            info = self.store.info(after.id)
            if not info or not info['notes_channel']:
                return
            try:
                # Fetch the latest name so delayed events cannot undo a later rename.
                channel = await after.guild.fetch_channel(after.id)
                notes = await after.guild.fetch_channel(info['notes_channel'])
                if notes.name != channel.name:
                    await notes.edit(name=channel.name, reason='Match staff channel to ticket name')
            except discord.HTTPException as exc:
                logger.error('Staff channel rename failed: %s status=%s code=%s',
                             type(exc).__name__, exc.status, exc.code)

    def check_staff(self, interaction):
        if interaction.guild_id != GUILD_ID or not isinstance(interaction.user, discord.Member) or not staff(interaction.user):
            raise TicketError('この窓口は指定サーバーのスタッフ専用です。')

    def authorize_close(self, interaction):
        row = self.store.get(interaction.channel_id)
        member = interaction.user
        channel = interaction.channel
        if interaction.guild_id != GUILD_ID or not row or not isinstance(member, discord.Member) or not isinstance(channel, discord.TextChannel):
            raise TicketError('このチャンネルは登録済みチケットではありません。')
        owner, key, excluded = row
        if member.id == excluded or not channel.permissions_for(member).view_channel:
            raise TicketError('このチケットを閉じる権限がありません。')
        if member.id != owner and not any(r.id in DESKS[key].roles for r in member.roles):
            raise TicketError('相談者または担当者だけが閉じられます。')
        return row

    async def create_ticket(self, interaction, key, subject, content, party_text):
        self.check_staff(interaction)
        guild = interaction.guild
        desk = DESKS[key]
        party_id = parse_member_id(party_text) if desk.urgent else None
        party = None
        if party_id:
            if party_id in (interaction.user.id, self.user.id):
                raise TicketError('相談者自身またはBOTを当事者として指定できません。')
            try:
                party = await guild.fetch_member(party_id)
            except discord.NotFound:
                raise TicketError('当事者をサーバー内で確認できません。IDを確認してください。')
            if party.id == guild.owner_id or party.guild_permissions.administrator:
                raise TicketError('当事者が所有者または管理者のため、Discordでは閲覧を遮断できません。相談内容は投稿していません。別の安全な相談経路を使用してください。')
        roles = [guild.get_role(rid) for rid in desk.roles]
        if any(r is None or r.is_default() for r in roles):
            raise TicketError('担当ロールの設定を確認できません。管理者へ設定確認を依頼してください。')
        category = guild.get_channel(CATEGORY_ID)
        if not isinstance(category, discord.CategoryChannel):
            raise TicketError('チケット用カテゴリの設定が不正です。')
        me = guild.me
        required = ('manage_channels', 'manage_roles', 'view_channel', 'send_messages', 'embed_links', 'read_message_history', 'attach_files')
        perms = category.permissions_for(me)
        if not all(getattr(perms, name) for name in required):
            raise TicketError('BOTのカテゴリ権限が不足しています。')
        # For reports, fail closed if nobody other than reporter/party/bot can handle it.
        if desk.urgent:
            eligible = False
            async for member in guild.fetch_members(limit=None):
                if not member.bot and member.id not in (interaction.user.id, party_id) and any(r.id in desk.roles for r in member.roles):
                    eligible = True
                    break
            if not eligible:
                raise TicketError('当事者以外の対応担当者が確認できないため作成を中止しました。別の安全な相談経路を使用してください。')
        async with self.create_lock:
            remaining = self.store.cooldown_remaining(interaction.user.id)
            if remaining:
                raise TicketError(f'連続作成を制限しています。あと{remaining}秒待ってください。')
            open_rows = self.store.db.execute('SELECT channel FROM tickets WHERE owner=? AND status != \'archived\'', (interaction.user.id,)).fetchall()
            count = 0
            for (cid,) in open_rows:
                try:
                    await guild.fetch_channel(cid)
                    count += 1
                except discord.NotFound:
                    self.store.remove(cid)
            if count >= MAX_OPEN:
                raise TicketError(f'同時に作成できるチケットは{MAX_OPEN}件までです。')
            allow = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True,
                                                attach_files=True, embed_links=True, create_public_threads=False,
                                                create_private_threads=False, manage_channels=False, manage_roles=False,
                                                mention_everyone=False, manage_webhooks=False)
            overwrites = {guild.default_role: discord.PermissionOverwrite(view_channel=False), me: discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True, embed_links=True,
                attach_files=True, manage_channels=True, manage_roles=True)}
            for role in roles:
                overwrites[role] = allow
            overwrites[interaction.user] = allow
            if party:
                # Explicit member deny overrides role allows, except Discord administrator/owner bypass.
                overwrites[party] = discord.PermissionOverwrite(view_channel=False, read_message_history=False, send_messages=False)
            channel = await guild.create_text_channel('ticket-' + os.urandom(4).hex(), category=category,
                overwrites=overwrites, reason='Confidential staff ticket')
            try:
                self.store.add(channel.id, interaction.user.id, key, party_id)
                if party and channel.permissions_for(party).view_channel:
                    raise TicketError('当事者の閲覧拒否を確認できなかったため作成を中止しました。')
                embed = discord.Embed(title=subject, description=content, color=0xB88A69,
                                      timestamp=datetime.now(timezone.utc))
                embed.add_field(name='窓口', value=desk.label, inline=False)
                if party:
                    embed.add_field(name='当事者（個別閲覧拒否）', value=f'{discord.utils.escape_markdown(party.display_name)} / ID: {party.id}', inline=False)
                # Urgent role text is displayed but role pings are suppressed to avoid notifying an excluded member.
                mentions = discord.AllowedMentions(everyone=False, users=[interaction.user], roles=roles if not desk.urgent else [], replied_user=False)
                await channel.send('状態：未対応\n' + ' '.join([interaction.user.mention] + [r.mention for r in roles]),
                                   embed=embed, view=CloseView(), allowed_mentions=mentions)
            except Exception:
                try:
                    await channel.delete(reason='Ticket initialization failed')
                    self.store.remove(channel.id)
                except discord.HTTPException:
                    pass  # Persisted record permits later manual cleanup. Never post contents to logs.
                raise
            self.store.created_now(interaction.user.id)
            self.store.audit(channel.id, interaction.user.id, 'created', key)
            return channel

    async def staff_ticket(self, interaction):
        channel, member, info = await self.fresh_ticket(interaction, handler=True)
        if member.id == info['owner']:
            raise TicketError('担当者用パネルは相談者本人以外の担当部門が利用できます。')
        return channel, member, info

    async def notes_overwrites(self, guild, info, target):
        owner = await guild.fetch_member(info['owner'])
        if owner.id == guild.owner_id or owner.guild_permissions.administrator:
            raise TicketError('相談者が所有者または管理者のため、内部メモを非公開にできません。')
        # Use the same owner/excluded member denies as an archived ticket.
        overwrites = await self.routing_overwrites(guild, info, dict(target, status='archived'))
        for rid in DESKS[target['desk']].roles:
            role = guild.get_role(rid)
            if target['status'] != 'archived':
                overwrites[role].send_messages = True
                overwrites[role].attach_files = True
                overwrites[role].embed_links = True
                overwrites[role].add_reactions = True
        return overwrites

    async def staff_notes(self, interaction):
        async with self.close_lock:
            channel, member, info = await self.staff_ticket(interaction)
            target = dict(status=info['status'], desk=info['desk'], sensitive=info['sensitive'])
            overwrites = await self.notes_overwrites(channel.guild, info, target)
            if info['notes_channel']:
                notes = await channel.guild.fetch_channel(info['notes_channel'])
                notes = await notes.edit(name=channel.name, overwrites=overwrites, reason='Verify staff ticket access')
                self.store.audit(channel.id, member.id, 'notes_opened')
            else:
                category = channel.category or channel.guild.get_channel(CATEGORY_ID)
                if not isinstance(category, discord.CategoryChannel):
                    raise TicketError('内部メモ作成先のカテゴリが見つかりません。')
                notes = await channel.guild.create_text_channel(channel.name,
                    category=category, overwrites=overwrites, reason='Private ticket staff notes')
                try:
                    self.store.update(channel.id, notes_channel=notes.id, actor=member.id, event='notes_created')
                except Exception:
                    # If registration fails, do not post any consultation details.
                    await notes.delete(reason='Staff channel registration failed')
                    raise
                await notes.send('担当者用の内部メモです。相談者と当事者には閲覧拒否を設定しています。\n'
                                 '窓口変更時は、相談者の承認後にメモの全履歴も変更先の担当部門へ引き継ぎます。\n'
                                 '相談チャンネルの完全削除時に、この内部メモも削除します。')
            await reply(interaction, f'スタッフ専用チャンネル：{notes.mention}')

    async def assign_member(self, interaction, member_id):
        async with self.close_lock:
            channel, actor, info = await self.staff_ticket(interaction)
            if info['status'] == 'archived':
                raise TicketError('保管中です。先に再開してください。')
            member = await channel.guild.fetch_member(member_id)
            if (member.bot or member.id in (info['owner'], info['excluded'])
                or not any(r.id in DESKS[info['desk']].roles for r in member.roles)
                or not channel.permissions_for(member).view_channel):
                raise TicketError('現在の担当部門に所属し、閲覧できる人を選択してください。相談者・当事者・BOTは指定できません。')
            self.store.update(channel.id, status='active', assignee=member.id,
                              actor=actor.id, event='assigned', subject_id=member.id)
            await channel.send(f'担当者を変更しました：<@{member.id}>／状態：対応中', allowed_mentions=discord.AllowedMentions.none())
            await reply(interaction, '担当者を変更しました。')

    async def show_audit(self, interaction):
        async with self.close_lock:
            channel, _, _ = await self.staff_ticket(interaction)
            rows = self.store.db.execute(
                'SELECT at,actor,event,desk,subject_id FROM ticket_audit WHERE ticket=? ORDER BY id DESC LIMIT 20', (channel.id,)).fetchall()
            labels = {'created':'作成', 'claimed':'受付', 'released':'受付解除', 'assigned':'担当変更',
                      'archived':'対応完了・保管', 'reopened':'再開', 'transferred':'部門変更',
                      'transition_requested':'権限変更開始', 'transition_applied':'権限変更完了',
                      'transfer_proposed':'部門変更申請', 'transfer_rejected':'部門変更拒否',
                      'notes_created':'内部メモ作成', 'notes_opened':'内部メモ表示',
                      'delete_requested':'完全削除開始', 'deleted':'完全削除',
                      'channel_removed':'チャンネル削除検知', 'retention_notice':'保管期限通知', 'notes_deleted':'内部メモ削除'}
            lines = ['操作履歴（最新20件・日本時間）']
            for at, actor, event, desk, subject in rows:
                stamp = datetime.fromtimestamp(at, ZoneInfo('Asia/Tokyo')).strftime('%m/%d %H:%M')
                line = f'{stamp} {labels.get(event,event)}／操作ID:{actor or "BOT"}'
                if desk: line += f'／窓口:{DESKS[desk].label}'
                if subject: line += f'／対象ID:{subject}'
                lines.append(line)
            # Use an embed to stay comfortably under Discord's 2000-character message cap.
            await interaction.followup.send(embed=discord.Embed(title='担当者用パネル｜操作履歴',
                description='\n'.join(lines)[:4000], color=0xB88A69), ephemeral=True)

    async def retention_once(self):
        if not RETENTION_DAYS:
            return
        guild = self.get_guild(GUILD_ID)
        if not guild:
            return
        now = time.time()
        ids = [r[0] for r in self.store.db.execute(
            "SELECT channel FROM tickets WHERE status='archived' AND archived_at>0 AND archived_at<=? AND retention_notified<=?",
            (now - RETENTION_DAYS*86400, now - 86400))]
        for cid in ids:
            async with self.close_lock:
                info = self.store.info(cid)
                if not info or info['status'] != 'archived':
                    continue
                try:
                    channel = await guild.fetch_channel(cid)
                    recipients = []
                    async for member in guild.fetch_members(limit=None):
                        if (not member.bot and member.id not in (info['owner'], info['excluded'])
                            and any(r.id in DESKS[info['desk']].roles for r in member.roles)
                            and channel.permissions_for(member).view_channel):
                            recipients.append(member)
                            if len(recipients) >= 20: break
                    await channel.send(' '.join(m.mention for m in recipients) +
                        f'\n保管期限（{RETENTION_DAYS}日）を迎えました。担当者用パネルで内容を確認し、必要なら完全削除してください。自動削除は行いません。',
                        view=CloseView(), allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=recipients))
                    self.store.update(cid, retention_notified=now, event='retention_notice')
                except discord.NotFound:
                    self.store.remove(cid)
                except Exception as exc:
                    logger.error('Retention notice failed: %s', type(exc).__name__)

    async def delete_staff_notes(self, channel, info, actor):
        if not info['notes_channel']:
            return
        try:
            notes = await channel.guild.fetch_channel(info['notes_channel'])
            await notes.delete(reason='Ticket and staff notes deletion confirmed')
        except discord.NotFound:
            pass
        self.store.update(channel.id, notes_channel=None, actor=actor, event='notes_deleted')

    async def cleanup_orphan_notes(self):
        guild = self.get_guild(GUILD_ID)
        if not guild:
            return
        for cid, ticket in self.store.db.execute('SELECT channel,ticket FROM orphan_notes').fetchall():
            try:
                notes = await guild.fetch_channel(cid)
                await notes.delete(reason='Parent ticket was deleted')
            except discord.NotFound:
                pass
            except Exception as exc:
                logger.error('Staff notes cleanup failed: %s', type(exc).__name__)
                continue
            self.store.audit(ticket, None, 'notes_deleted')
            self.store.db.execute('DELETE FROM orphan_notes WHERE channel=?', (cid,))
            self.store.db.commit()

    async def fresh_ticket(self, interaction, handler=False, owner_only=False, allow_pending=False):
        if interaction.guild_id != GUILD_ID:
            raise TicketError('対象サーバーではありません。')
        info = self.store.info(interaction.channel_id)
        if not info:
            raise TicketError('登録済みのチケットで実行してください。')
        channel = await interaction.guild.fetch_channel(interaction.channel_id)
        member = await interaction.guild.fetch_member(interaction.user.id)
        if not isinstance(channel, discord.TextChannel) or member.bot or member.id == info['excluded']:
            raise TicketError('操作する権限がありません。')
        if not channel.permissions_for(member).view_channel:
            raise TicketError('操作する権限がありません。')
        is_owner = member.id == info['owner']
        if is_owner and info['status'] == 'archived':
            raise TicketError('保管済みのチケットは相談者本人から操作できません。担当部門へ依頼してください。')
        is_handler = any(r.id in DESKS[info['desk']].roles for r in member.roles)
        if (owner_only and not is_owner) or (handler and not is_handler) or not (is_owner or is_handler):
            raise TicketError('相談者または現在の担当部門だけが操作できます。')
        if info['status'] == 'updating' and not allow_pending:
            raise TicketError('権限更新が未完了です。管理者に /ticket_recover を依頼してください。')
        return channel, member, info

    async def routing_overwrites(self, guild, info, target):
        roles = [guild.get_role(rid) for rid in DESKS[target['desk']].roles]
        if any(r is None or r.is_default() for r in roles):
            raise TicketError('担当ロールが見つからないため権限を変更しません。')
        owner = await guild.fetch_member(info['owner'])
        if target['status'] == 'archived' and (owner.id == guild.owner_id or owner.guild_permissions.administrator):
            raise TicketError('相談者が所有者または管理者のため、Discordでは非表示にできません。権限を確認するまで対応完了を中止します。')
        party = None
        if info['excluded']:
            try:
                party = await guild.fetch_member(info['excluded'])
            except discord.NotFound:
                pass  # Retain an ID overwrite so a rejoining member is still denied.
            if party and (party.id == guild.owner_id or party.guild_permissions.administrator):
                raise TicketError('当事者が所有者・管理者のため権限変更を中止しました。別の安全な相談経路が必要です。')
        if target['sensitive']:
            eligible = False
            async for member in guild.fetch_members(limit=None):
                if not member.bot and member.id not in (info['owner'], info['excluded']) and any(r.id in DESKS[target['desk']].roles for r in member.roles):
                    eligible = True
                    break
            if not eligible:
                raise TicketError('当事者・相談者以外の担当者がいないため変更できません。')
        writable = target['status'] != 'archived'
        allow = discord.PermissionOverwrite(
            view_channel=True, read_message_history=True, send_messages=writable,
            send_messages_in_threads=False, add_reactions=writable,
            attach_files=writable, embed_links=writable, create_public_threads=False,
            create_private_threads=False, manage_channels=False, manage_roles=False,
            mention_everyone=False, manage_webhooks=False)
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False, send_messages=False,
                                                           send_messages_in_threads=False),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True,
                read_message_history=True, embed_links=True, attach_files=True,
                manage_channels=True, manage_roles=True),
            owner: allow,
        }
        overwrites.update({role: allow for role in roles})
        if not writable:
            # A member deny wins even if the reporter also has a handler role.
            overwrites[owner] = discord.PermissionOverwrite(
                view_channel=False, read_message_history=False, send_messages=False,
                send_messages_in_threads=False, add_reactions=False)
        if info['excluded']:
            overwrites[party or discord.Object(id=info['excluded'], type=discord.Member)] = discord.PermissionOverwrite(
                view_channel=False, read_message_history=False, send_messages=False, send_messages_in_threads=False)
        return overwrites

    async def apply_transition(self, channel, info, target, actor=None):
        # Persist the approved target BEFORE the external API call. A crash or
        # ambiguous HTTP failure leaves a recoverable state, never a stale active route.
        overwrites = await self.routing_overwrites(channel.guild, info, target)
        notes = None
        notes_overwrites = None
        if info['notes_channel']:
            notes = await channel.guild.fetch_channel(info['notes_channel'])
            notes_overwrites = await self.notes_overwrites(channel.guild, info, target)
        self.store.update(channel.id, status='updating', pending=json.dumps(target), actor=actor, event='transition_requested')
        if notes:
            await notes.edit(name=channel.name, overwrites=notes_overwrites, reason='Staff ticket permissions update')
        updated = await channel.edit(overwrites=overwrites, reason='Ticket workflow permission update')
        event = 'transferred' if target['desk'] != info['desk'] else ('archived' if target['status'] == 'archived' else 'reopened')
        self.store.update(channel.id, **target, pending=None, actor=actor, event=event)
        return updated

    async def change_archive(self, interaction, archive):
        channel, member, info = await self.fresh_ticket(interaction)
        if archive and info['status'] == 'archived':
            raise TicketError('すでに保管されています。')
        if not archive:
            if info['status'] != 'archived':
                raise TicketError('保管中のチケットだけを再開できます。')
            # Keep create + reopen under the same quota lock (taken by caller).
            count = self.store.db.execute("SELECT COUNT(*) FROM tickets WHERE owner=? AND status!='archived'", (info['owner'],)).fetchone()[0]
            if count >= MAX_OPEN:
                raise TicketError('同時チケット数の上限です。他のチケットを保管してください。')
        target = dict(status='archived' if archive else 'waiting', desk=info['desk'],
                      sensitive=info['sensitive'], assignee=None,
                      created_at=time.time(), notified_at=0,
                      archived_at=time.time() if archive else 0, retention_notified=0)
        channel = await self.apply_transition(channel, info, target, actor=member.id)
        await channel.send('対応完了として保管しました。' if archive else '再開しました。状態：未対応', view=CloseView())

    async def claim(self, interaction, release=False):
        async with self.close_lock:
            channel, member, info = await self.fresh_ticket(interaction, handler=True)
            if info['status'] == 'archived':
                raise TicketError('保管中です。先に再開してください。')
            if info['assignee'] and info['assignee'] != member.id:
                raise TicketError('別の担当者が受付済みです。')
            if release and info['assignee'] != member.id:
                raise TicketError('自分が担当しているチケットだけを解除できます。')
            if info['assignee'] == member.id and not release:
                raise TicketError('すでにあなたが担当しています。解除は担当者用パネルから行ってください。')
            if release:
                self.store.update(channel.id, status='waiting', assignee=None, created_at=time.time(), notified_at=0, actor=member.id, event='released')
                await channel.send('受付を解除しました。状態：未対応')
            else:
                if member.id == info['owner']:
                    raise TicketError('相談者自身は受付担当になれません。')
                self.store.update(channel.id, status='active', assignee=member.id, actor=member.id, event='claimed')
                await channel.send(f'受付しました。状態：対応中／担当：<@{member.id}>', allowed_mentions=discord.AllowedMentions.none())

    async def send_status(self, interaction):
        async with self.close_lock:
            _, _, info = await self.fresh_ticket(interaction, allow_pending=True)
            label = STATE_LABELS.get(info['status'], '不明')
            assignee = f"<@{info['assignee']}>" if info['assignee'] else '未割当'
            await reply(interaction, f"状態：{label}\n窓口：{DESKS[info['desk']].label}\n担当：{assignee}")

    @tasks.loop(minutes=1)
    async def reminders(self):
        try:
            await self.remind_once()
            await self.retention_once()
            await self.cleanup_orphan_notes()
        except Exception as exc:
            logger.error('Reminder scan failed: %s', type(exc).__name__)

    @reminders.before_loop
    async def before_reminders(self):
        await self.wait_until_ready()

    async def remind_once(self):
        if not REMIND_MINUTES:
            return
        guild = self.get_guild(GUILD_ID)
        if not guild:
            return
        threshold = time.time() - REMIND_MINUTES * 60
        ids = [r[0] for r in self.store.db.execute(
            "SELECT channel FROM tickets WHERE status='waiting' AND created_at<=? AND notified_at<=?", (threshold, threshold))]
        for cid in ids:
            async with self.close_lock:
                info = self.store.info(cid)
                if not info or info['status'] != 'waiting':
                    continue
                try:
                    channel = await guild.fetch_channel(cid)
                    # Individual eligible recipients only. No role ping, no DMs,
                    # and no message to a broader operational channel.
                    recipients = []
                    async for member in guild.fetch_members(limit=None):
                        if (not member.bot and member.id not in (info['owner'], info['excluded'])
                            and any(r.id in DESKS[info['desk']].roles for r in member.roles)
                            and channel.permissions_for(member).view_channel):
                            recipients.append(member)
                            if len(recipients) >= 20:
                                break
                    text = ' '.join(m.mention for m in recipients)
                    await channel.send(text + '\n未対応の相談です。担当者は「受付／解除」を押してください。',
                        allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=recipients))
                    self.store.update(cid, notified_at=time.time())
                except discord.NotFound:
                    self.store.remove(cid)
                except Exception as exc:
                    logger.error('Reminder failed: %s', type(exc).__name__)

    async def propose_transfer(self, interaction, key):
        async with self.close_lock:
            channel, _, info = await self.fresh_ticket(interaction)
            if info['status'] == 'archived' or key == info['desk']:
                raise TicketError('稼働中のチケットで別の部門を選択してください。')
            # Preflight WITHOUT granting access; approval revalidates everything.
            target = dict(status='waiting', desk=key, sensitive=int(info['sensitive'] or DESKS[key].urgent),
                          assignee=None, created_at=time.time(), notified_at=0)
            await self.routing_overwrites(channel.guild, info, target)
            await channel.send(
                f"相談者の承認待ち：窓口を「{DESKS[key].label}」へ変更します。\n"
                '承認すると、このチャンネルの過去の相談内容・添付を含む全履歴を変更先の担当部門に共有します。'
                '内部メモ用チャンネルがある場合は、そのメモの全履歴も変更先の担当部門へ引き継ぎます。'
                '元の部門の閲覧許可は外れます（両部門に所属する人と管理者は例外）。当事者の閲覧拒否は維持します。\n'
                '相談者本人だけが承認・拒否できます。3分で失効します。',
                view=TransferApproval(info['owner'], info['revision'], key))
            self.store.audit(channel.id, interaction.user.id, 'transfer_proposed', key)

    async def approve_transfer(self, interaction, revision, key):
        async with self.close_lock:
            channel, _, info = await self.fresh_ticket(interaction, owner_only=True)
            if info['revision'] != revision or info['status'] == 'archived':
                raise TicketError('状態が変更されたため申請が失効しました。改めて申請してください。')
            target = dict(status='waiting', desk=key, sensitive=int(info['sensitive'] or DESKS[key].urgent),
                          assignee=None, created_at=time.time(), notified_at=0)
            channel = await self.apply_transition(channel, info, target, actor=interaction.user.id)
            await channel.send(f'相談者の承認により窓口を「{DESKS[key].label}」へ変更しました。状態：未対応', view=CloseView())

    async def config_check(self, interaction):
        if (interaction.guild_id != GUILD_ID or not isinstance(interaction.user, discord.Member)
            or not interaction.user.guild_permissions.manage_guild):
            raise TicketError('サーバー管理権限が必要です。')
        guild = interaction.guild
        lines = [f'実行環境：{TICKET_ENV}／v0.3.1']
        category = guild.get_channel(CATEGORY_ID)
        lines.append('カテゴリ：' + ('OK' if isinstance(category, discord.CategoryChannel) else '見つかりません'))
        if isinstance(category, discord.CategoryChannel):
            perms = category.permissions_for(guild.me)
            for key in ('view_channel','manage_channels','manage_roles','send_messages','embed_links','read_message_history','attach_files'):
                lines.append(f'{key}：' + ('OK' if getattr(perms, key) else '不足'))
        for key, desk in DESKS.items():
            roles = [guild.get_role(rid) for rid in desk.roles]
            missing = any(r is None or r.is_default() for r in roles)
            lines.append(desk.label + '：' + ('ロール不足' if missing else 'OK'))
            if not missing and any(r >= guild.me.top_role for r in roles):
                lines.append('  BOTより上位または同順位の担当ロールあり。ロール順位を確認してください。')
        lines.append('利用スタッフロール：' + ('OK' if all(guild.get_role(rid) for rid in STAFF_IDS) else '不足'))
        try:
            async for _ in guild.fetch_members(limit=1):
                break
            lines.append('メンバー一覧取得：OK')
        except discord.HTTPException:
            lines.append('メンバー一覧取得：失敗（メンバーインテント等を確認）')
        lines.append(f'未対応通知：{REMIND_MINUTES}分間隔（0は無効）。BOT稼働中のみ')
        if LOG_ID:
            log = await guild.fetch_channel(LOG_ID)
            ok = isinstance(log, discord.TextChannel) and all(getattr(log.permissions_for(guild.me), p) for p in ('view_channel','send_messages','attach_files'))
            lines.append('削除時ログ送信権限：' + ('OK' if ok else '不足'))
            lines.append('ログ閲覧範囲は削除実行時に個別照合します。')
        else:
            lines.append('削除時ログ：無効')
        pending = self.store.db.execute("SELECT COUNT(*) FROM tickets WHERE status='updating'").fetchone()[0]
        lines.append(f'権限更新未完了：{pending}件（該当チャンネルで /ticket_recover）')
        orphan_count = self.store.db.execute('SELECT COUNT(*) FROM orphan_notes').fetchone()[0]
        lines.append(f'内部メモ削除再試行待ち：{orphan_count}件')
        lines.append(f'連続作成制限：{COOLDOWN_SECONDS}秒（0は無効）')
        lines.append(f'保管期限：{RETENTION_DAYS}日（0は通知無効・自動削除なし）')
        await reply(interaction, '\n'.join(lines))

    async def transcript(self, channel, row):
        # Harassment reports NEVER go to a shared operational log channel.
        if not LOG_ID or self.store.info(channel.id)['sensitive']:
            return
        log = await channel.guild.fetch_channel(LOG_ID)
        if not isinstance(log, discord.TextChannel) or log.id == channel.id:
            raise TicketError('ログチャンネルの設定が不正なため削除を中止しました。')
        if not all(getattr(log.permissions_for(channel.guild.me), p) for p in ('view_channel', 'send_messages', 'attach_files')):
            raise TicketError('ログの送信権限が不足しているため削除を中止しました。')
        # Fail closed if the log audience is broader than this ticket's audience.
        async for member in channel.guild.fetch_members(limit=None):
            if log.permissions_for(member).view_channel and not channel.permissions_for(member).view_channel:
                raise TicketError('ログ閲覧者が相談窓口より広いため削除を中止しました。ログ設定を見直してください。')
        lines = [f'Ticket ID: {channel.id} (UTC)', '添付はURLのみ。画像・ファイル本体は保存しません。']
        size = 0
        async for msg in channel.history(limit=None, oldest_first=True):
            line = f'[{msg.created_at.isoformat()}] {msg.author.id}: {msg.content}\n'
            line += ''.join(f'[添付] {a.url}\n' for a in msg.attachments)
            line += ''.join('[Embed] ' + json.dumps(e.to_dict(), ensure_ascii=False) + '\n' for e in msg.embeds)
            size += len(line.encode('utf-8'))
            if size > min(channel.guild.filesize_limit - 4096, 7_000_000):
                raise TicketError('ログが大きすぎるため削除を中止しました。分割保存を実装してください。')
            lines.append(line)
        await log.send(f'チケット記録: {channel.id}', file=discord.File(io.BytesIO('\n'.join(lines).encode('utf-8')), filename=f'ticket-{channel.id}.txt'),
                       allowed_mentions=discord.AllowedMentions.none())


class RequestModal(discord.ui.Modal):
    def __init__(self, key, party_id=None):
        super().__init__(title='相談内容の入力', timeout=300)
        self.key = key
        self.party_id = party_id if DESKS[key].urgent else None
        self.subject = discord.ui.TextInput(label='件名', max_length=150)
        self.content = discord.ui.TextInput(label='相談内容', style=discord.TextStyle.paragraph, max_length=3500)
        self.add_item(self.subject)
        self.add_item(self.content)

    async def on_submit(self, interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            channel = await interaction.client.create_ticket(interaction, self.key, self.subject.value, self.content.value,
                str(self.party_id) if self.party_id is not None else '')
            await interaction.followup.send(f'相談窓口を作成しました：{channel.mention}', ephemeral=True)
        except (TicketError, ValueError) as exc:
            await reply(interaction, str(exc))
        except Exception as exc:
            logger.error('Ticket creation failed: %s', type(exc).__name__)
            await reply(interaction, '作成できませんでした。管理者へBOT設定の確認を依頼してください。')


class PartySelect(discord.ui.UserSelect):
    def __init__(self):
        super().__init__(placeholder='問題の当事者を1人選択', min_values=1, max_values=1)

    async def callback(self, interaction):
        try:
            interaction.client.check_staff(interaction)
            await interaction.response.send_modal(RequestModal('report', self.values[0].id))
        except TicketError as exc:
            await reply(interaction, str(exc))


class PartyView(discord.ui.View):
    def __init__(self, requester):
        super().__init__(timeout=180)
        self.requester = requester
        self.add_item(PartySelect())

    async def interaction_check(self, interaction):
        if interaction.user.id != self.requester:
            await reply(interaction, '相談を開始した本人だけが選択できます。')
            return False
        return True

    @discord.ui.button(label='当事者を指定せずに進む', style=discord.ButtonStyle.secondary)
    async def skip(self, interaction, button):
        try:
            interaction.client.check_staff(interaction)
            await interaction.response.send_modal(RequestModal('report'))
        except TicketError as exc:
            await reply(interaction, str(exc))


class DeskSelect(discord.ui.Select):
    def __init__(self):
        super().__init__(placeholder='相談窓口を選択', options=[discord.SelectOption(label=d.label, value=k) for k, d in DESKS.items()])

    async def callback(self, interaction):
        try:
            interaction.client.check_staff(interaction)
            key = self.values[0]
            if DESKS[key].urgent:
                await interaction.response.send_message(
                    '問題の当事者を選んでください。選択後に相談内容の入力フォームが開きます。',
                    view=PartyView(interaction.user.id), ephemeral=True)
            else:
                await interaction.response.send_modal(RequestModal(key))
        except TicketError as exc:
            await reply(interaction, str(exc))


class DeskView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=180)
        self.add_item(DeskSelect())


class PanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='相談窓口を開く', custom_id='concierge:open:v1', style=discord.ButtonStyle.primary)
    async def open(self, interaction, button):
        try:
            interaction.client.check_staff(interaction)
            await interaction.response.send_message('相談先を選択してください。サーバー所有者・管理者の閲覧はDiscord仕様上制限できません。', view=DeskView(), ephemeral=True)
        except TicketError as exc:
            await reply(interaction, str(exc))


class CloseView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='担当する', emoji='🙋', custom_id='concierge:claim:v1', style=discord.ButtonStyle.success)
    async def claim_ticket(self, interaction, button):
        async def action():
            await interaction.client.claim(interaction)
            await reply(interaction, '担当として受付しました。')
        await workflow_reply(interaction, action)

    @discord.ui.button(label='担当者用パネル', emoji='🛡️', custom_id='concierge:staffpanel:v1', style=discord.ButtonStyle.secondary)
    async def staff_panel(self, interaction, button):
        async def action():
            async with interaction.client.close_lock:
                _, _, info = await interaction.client.staff_ticket(interaction)
                embed = discord.Embed(title='担当者用パネル',
                    description='担当者だけが操作できます。\n内部メモ・担当交代・操作履歴・窓口変更・保管後の操作をまとめています。', color=0xB88A69)
                embed.add_field(name='状態', value=STATE_LABELS.get(info['status'], '不明'))
                await interaction.followup.send(embed=embed, view=StaffPanel(interaction.user.id), ephemeral=True)
        await workflow_reply(interaction, action)

    @discord.ui.button(label='チケットを閉じる', emoji='🔒', custom_id='concierge:close:v1', style=discord.ButtonStyle.danger)
    async def close_ticket(self, interaction, button):
        try:
            interaction.client.authorize_close(interaction)
            await interaction.response.send_message('対応を完了し、相談者から非表示にして保管しますか？担当部門は閲覧でき、後で再開できます。',
                view=ConfirmClose(interaction.user.id), ephemeral=True)
        except TicketError as exc:
            await reply(interaction, str(exc))


class LegacyControls(discord.ui.View):
    """Keep old messages working after restart; new tickets use the three-button row."""
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='対応状況', custom_id='concierge:status:v1')
    async def status_ticket(self, interaction, button):
        await workflow_reply(interaction, lambda: interaction.client.send_status(interaction))

    @discord.ui.button(label='再開する', custom_id='concierge:reopen:v1')
    async def reopen_ticket(self, interaction, button):
        await staff_action(interaction, 'reopen')

    @discord.ui.button(label='窓口を変更', custom_id='concierge:transfer:v1')
    async def transfer_ticket(self, interaction, button):
        await staff_action(interaction, 'transfer')


class StaffPanel(discord.ui.View):
    def __init__(self, requester):
        super().__init__(timeout=180)
        self.requester = requester

    async def interaction_check(self, interaction):
        if interaction.user.id != self.requester:
            await reply(interaction, 'パネルを開いた担当者本人だけが操作できます。')
            return False
        # Each action also revalidates membership under the operation lock.
        try:
            await interaction.client.staff_ticket(interaction)
            return True
        except TicketError as exc:
            await reply(interaction, str(exc))
            return False

    @discord.ui.button(label='スタッフ専用チャンネル', emoji='🛡️', style=discord.ButtonStyle.secondary, row=0)
    async def notes(self, interaction, button):
        await workflow_reply(interaction, lambda: interaction.client.staff_notes(interaction))

    @discord.ui.button(label='担当者を変更', style=discord.ButtonStyle.primary, row=0)
    async def assign(self, interaction, button):
        await staff_action(interaction, 'assign')

    @discord.ui.button(label='受付を解除', style=discord.ButtonStyle.secondary, row=0)
    async def release(self, interaction, button):
        await staff_action(interaction, 'release')

    @discord.ui.button(label='操作履歴', style=discord.ButtonStyle.secondary, row=1)
    async def audit(self, interaction, button):
        await workflow_reply(interaction, lambda: interaction.client.show_audit(interaction))

    @discord.ui.button(label='対応状況', style=discord.ButtonStyle.secondary, row=1)
    async def status(self, interaction, button):
        await staff_action(interaction, 'status')

    @discord.ui.button(label='窓口を変更', style=discord.ButtonStyle.secondary, row=1)
    async def transfer(self, interaction, button):
        await staff_action(interaction, 'transfer')

    @discord.ui.button(label='再開する', style=discord.ButtonStyle.success, row=2)
    async def reopen(self, interaction, button):
        await staff_action(interaction, 'reopen')

    @discord.ui.button(label='完全削除', style=discord.ButtonStyle.danger, row=2)
    async def delete(self, interaction, button):
        await staff_action(interaction, 'delete')


async def staff_action(interaction, action):
    async def perform():
        bot = interaction.client
        if action == 'reopen':
            async with bot.create_lock:
                async with bot.close_lock:
                    await bot.staff_ticket(interaction)
                    await bot.change_archive(interaction, False)
                    await reply(interaction, '再開しました。相談者の閲覧を復元しました。')
        elif action == 'release':
            await bot.staff_ticket(interaction)
            await bot.claim(interaction, release=True)
            await reply(interaction, '受付を解除しました。')
        elif action == 'status':
            await bot.staff_ticket(interaction)
            await bot.send_status(interaction)
        else:
            async with bot.close_lock:
                _, _, info = await bot.staff_ticket(interaction)
                if action == 'assign':
                    if info['status'] == 'archived': raise TicketError('先に再開してください。')
                    await interaction.followup.send('同じ担当部門の交代先を選択してください。', view=AssignView(interaction.user.id), ephemeral=True)
                elif action == 'transfer':
                    if info['status'] == 'archived': raise TicketError('先に再開してください。')
                    await interaction.followup.send('窓口変更は相談者の承認後に実行します。',
                        view=TransferView(interaction.user.id, info['desk']), ephemeral=True)
                elif action == 'delete':
                    if info['status'] != 'archived': raise TicketError('先にチケットを閉じて保管してください。')
                    await interaction.followup.send('相談内容と内部メモを完全削除します。元に戻せません。操作履歴のメタデータは残ります。\n'
                        '緊急相談・ログ無効時は相談記録を外部保存しません。本当に削除しますか？',
                        view=ConfirmDelete(interaction.user.id, info['revision']), ephemeral=True)
    await workflow_reply(interaction, perform)


class AssignSelect(discord.ui.UserSelect):
    def __init__(self):
        super().__init__(placeholder='交代先の担当者を選択', min_values=1, max_values=1)

    async def callback(self, interaction):
        await workflow_reply(interaction, lambda: interaction.client.assign_member(interaction, self.values[0].id))


class AssignView(discord.ui.View):
    def __init__(self, requester):
        super().__init__(timeout=180)
        self.requester = requester
        self.add_item(AssignSelect())

    async def interaction_check(self, interaction):
        if interaction.user.id != self.requester:
            await reply(interaction, '担当者変更を開始した本人だけが操作できます。')
            return False
        return True


async def workflow_reply(interaction, action):
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        await action()
    except TicketError as exc:
        await reply(interaction, str(exc))
    except Exception as exc:
        logger.error('Ticket workflow failed: %s status=%s code=%s',
                     type(exc).__name__, getattr(exc, 'status', '-'), getattr(exc, 'code', '-'))
        await reply(interaction, '処理に失敗しました。/ticket_status を確認し、権限更新が未完了なら管理者へ連絡してください。')


class TransferSelect(discord.ui.Select):
    def __init__(self, current):
        super().__init__(placeholder='変更先の窓口を選択', options=[
            discord.SelectOption(label=d.label, value=k) for k, d in DESKS.items() if k != current])

    async def callback(self, interaction):
        async def action():
            await interaction.client.propose_transfer(interaction, self.values[0])
            await reply(interaction, 'チケット内に相談者の承認ボタンを表示しました。承認までは共有先を変更しません。')
        await workflow_reply(interaction, action)


class TransferView(discord.ui.View):
    def __init__(self, requester, current):
        super().__init__(timeout=180)
        self.requester = requester
        self.add_item(TransferSelect(current))

    async def interaction_check(self, interaction):
        if interaction.user.id != self.requester:
            await reply(interaction, '変更を申請した本人だけが選択できます。')
            return False
        return True


class TransferApproval(discord.ui.View):
    def __init__(self, owner, revision, key):
        super().__init__(timeout=180)
        self.owner, self.revision, self.key = owner, revision, key
        self.used = False

    async def interaction_check(self, interaction):
        if interaction.user.id != self.owner or self.used:
            await reply(interaction, '相談者本人だけが操作できます。処理済みの申請は再利用できません。')
            return False
        return True

    @discord.ui.button(label='全履歴の共有を承認', style=discord.ButtonStyle.danger)
    async def approve(self, interaction, button):
        self.used = True
        self.stop()
        async def action():
            await interaction.client.approve_transfer(interaction, self.revision, self.key)
            await reply(interaction, '窓口を変更しました。')
        await workflow_reply(interaction, action)

    @discord.ui.button(label='共有を拒否', style=discord.ButtonStyle.secondary)
    async def reject(self, interaction, button):
        self.used = True
        self.stop()
        interaction.client.store.audit(interaction.channel_id, interaction.user.id, 'transfer_rejected', self.key)
        await interaction.response.edit_message(content='窓口変更は相談者により拒否されました。閲覧範囲は変更していません。', view=None)


class ConfirmDelete(discord.ui.View):
    def __init__(self, requester, revision):
        super().__init__(timeout=60)
        self.requester, self.revision = requester, revision
        self.used = False

    async def interaction_check(self, interaction):
        if interaction.user.id != self.requester or self.used:
            await reply(interaction, '確認した本人だけが1回実行できます。')
            return False
        return True

    @discord.ui.button(label='完全削除する', style=discord.ButtonStyle.danger)
    async def confirm(self, interaction, button):
        self.used = True
        self.stop()
        async def action():
            bot = interaction.client
            async with bot.close_lock:
                channel, _, info = await bot.fresh_ticket(interaction)
                if info['status'] != 'archived' or info['revision'] != self.revision:
                    raise TicketError('状態が変更されました。保管後に改めて削除してください。')
                await bot.transcript(channel, (info['owner'], info['desk'], info['excluded']))
                bot.store.audit(channel.id, interaction.user.id, 'delete_requested')
                await bot.delete_staff_notes(channel, info, interaction.user.id)
                await channel.delete(reason='Archived ticket deletion confirmed')
                bot.store.audit(channel.id, interaction.user.id, 'deleted')
                bot.store.remove(channel.id)
                await reply(interaction, 'チケットを完全削除しました。')
        await workflow_reply(interaction, action)

    @discord.ui.button(label='キャンセル', style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction, button):
        self.used = True
        self.stop()
        await interaction.response.edit_message(content='削除をキャンセルしました。', view=None)


class ConfirmClose(discord.ui.View):
    def __init__(self, requester):
        super().__init__(timeout=60)
        self.requester = requester

    @discord.ui.button(label='はい、保管する', style=discord.ButtonStyle.primary)
    async def confirm(self, interaction, button):
        if interaction.user.id != self.requester:
            return await reply(interaction, '確認操作を行った本人だけが実行できます。')
        async def action():
            bot = interaction.client
            async with bot.close_lock:
                await bot.change_archive(interaction, True)
                self.stop()
                await reply(interaction, '対応完了として相談者から非表示にして保管しました。再開は担当部門が操作してください。')
        await workflow_reply(interaction, action)

    @discord.ui.button(label='キャンセル', style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction, button):
        if interaction.user.id != self.requester:
            return await reply(interaction, '確認操作を行った本人だけが実行できます。')
        self.stop()
        await interaction.response.edit_message(content='キャンセルしました。', view=None)


bot = TicketBot()


@bot.tree.command(name='ticket', description='社内相談窓口を開く', guild=discord.Object(id=GUILD_ID))
@app_commands.guild_only()
async def ticket(interaction: discord.Interaction):
    try:
        bot.check_staff(interaction)
        await interaction.response.send_message('相談先を選択してください。サーバー所有者・管理者の閲覧はDiscord仕様上制限できません。', view=DeskView(), ephemeral=True)
    except TicketError as exc:
        await reply(interaction, str(exc))


@bot.tree.command(name='ticket_panel', description='相談窓口パネルを設置する', guild=discord.Object(id=GUILD_ID))
@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
async def ticket_panel(interaction: discord.Interaction):
    if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.manage_guild:
        return await reply(interaction, 'サーバー管理権限が必要です。')
    await interaction.response.defer(ephemeral=True)
    await interaction.channel.send('社内相談窓口：担当部門に非公開で相談できます。\nサーバー所有者・管理者にはDiscord仕様上、閲覧を制限できません。', view=PanelView())
    await reply(interaction, 'パネルを設置しました。')


@bot.tree.command(name='ticket_status', description='現在の対応状況を確認', guild=discord.Object(id=GUILD_ID))
@app_commands.guild_only()
async def ticket_status(interaction: discord.Interaction):
    await workflow_reply(interaction, lambda: bot.send_status(interaction))


@bot.tree.command(name='ticket_check', description='BOTの設定と権限を確認', guild=discord.Object(id=GUILD_ID))
@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
async def ticket_check(interaction: discord.Interaction):
    await workflow_reply(interaction, lambda: bot.config_check(interaction))


@bot.tree.command(name='ticket_controls', description='既存チケットに操作ボタンを再表示', guild=discord.Object(id=GUILD_ID))
@app_commands.guild_only()
async def ticket_controls(interaction: discord.Interaction):
    async def action():
        async with bot.close_lock:
            channel, _, _ = await bot.fresh_ticket(interaction)
            await channel.send('チケット操作（最新の状態は「対応状況」で確認）', view=CloseView())
            await reply(interaction, '操作ボタンを表示しました。')
    await workflow_reply(interaction, action)


@bot.tree.command(name='ticket_delete', description='保管済みチケットを確認後に完全削除', guild=discord.Object(id=GUILD_ID))
@app_commands.guild_only()
async def ticket_delete(interaction: discord.Interaction):
    async def action():
        async with bot.close_lock:
            _, _, info = await bot.fresh_ticket(interaction)
            if info['status'] != 'archived':
                raise TicketError('先に「チケットを閉じる」で保管してください。')
            await interaction.followup.send(
                'このチャンネルと内部メモの全履歴を完全削除します。元に戻せません。操作履歴のメタデータは残ります。'
                '緊急相談やログ無効時は記録を保存しません。本当に削除しますか？',
                view=ConfirmDelete(interaction.user.id, info['revision']), ephemeral=True)
    await workflow_reply(interaction, action)


@bot.tree.command(name='ticket_recover', description='中断した承認済みの権限更新を再実行', guild=discord.Object(id=GUILD_ID))
@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
async def ticket_recover(interaction: discord.Interaction):
    async def action():
        if (interaction.guild_id != GUILD_ID or not isinstance(interaction.user, discord.Member)
            or not interaction.user.guild_permissions.manage_guild):
            raise TicketError('サーバー管理権限が必要です。')
        async with bot.create_lock:
            async with bot.close_lock:
                info = bot.store.info(interaction.channel_id)
                if not info or info['status'] != 'updating' or not info['pending']:
                    raise TicketError('このチャンネルに未完了の権限更新はありません。')
                if interaction.user.id == info['excluded']:
                    raise TicketError('当事者は操作できません。')
                channel = await interaction.guild.fetch_channel(interaction.channel_id)
                if not channel.permissions_for(interaction.user).view_channel:
                    raise TicketError('閲覧権限がありません。')
                await bot.apply_transition(channel, info, json.loads(info['pending']), actor=interaction.user.id)
                await reply(interaction, '承認済みの権限更新を再実行しました。/ticket_status で確認してください。')
    await workflow_reply(interaction, action)


if __name__ == '__main__':
    bot.run(os.environ['DISCORD_TOKEN'], log_handler=None)
