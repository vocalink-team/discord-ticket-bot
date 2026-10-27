"""Python 3.10+, discord.py 2.x. Run ONE process per SQLite database."""
import asyncio
import io
import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands

GUILD_ID = int(os.environ['GUILD_ID'])
CATEGORY_ID = int(os.environ['TICKET_CATEGORY_ID'])
STAFF_IDS = {int(x.strip()) for x in os.environ['STAFF_ROLE_IDS'].split(',') if x.strip()}
LOG_ID = int(os.getenv('TICKET_LOG_CHANNEL_ID', '0'))
DB_PATH = os.getenv('TICKET_DB_PATH', 'tickets.sqlite3')
MAX_OPEN = int(os.getenv('MAX_OPEN_TICKETS', '3'))
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
        self.db.commit()

    def get(self, cid):
        return self.db.execute('SELECT owner,desk,excluded FROM tickets WHERE channel=?', (cid,)).fetchone()

    def add(self, cid, owner, desk, excluded):
        self.db.execute('INSERT INTO tickets VALUES (?,?,?,?)', (cid, owner, desk, excluded))
        self.db.commit()

    def remove(self, cid):
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
        await self.tree.sync(guild=discord.Object(id=GUILD_ID))

    async def close(self):
        await super().close()
        self.store.db.close()

    async def on_guild_channel_delete(self, channel):
        if channel.guild.id == GUILD_ID:
            self.store.remove(channel.id)

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
        required = ('manage_channels', 'manage_roles', 'view_channel', 'send_messages', 'embed_links', 'read_message_history')
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
            open_rows = self.store.db.execute('SELECT channel FROM tickets WHERE owner=?', (interaction.user.id,)).fetchall()
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
                await channel.send(' '.join([interaction.user.mention] + [r.mention for r in roles]),
                                   embed=embed, view=CloseView(), allowed_mentions=mentions)
            except Exception:
                try:
                    await channel.delete(reason='Ticket initialization failed')
                    self.store.remove(channel.id)
                except discord.HTTPException:
                    pass  # Persisted record permits later manual cleanup. Never post contents to logs.
                raise
            return channel

    async def transcript(self, channel, row):
        # Harassment reports NEVER go to a shared operational log channel.
        if not LOG_ID or DESKS[row[1]].urgent:
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
    def __init__(self, key):
        super().__init__(title='相談内容の入力', timeout=300)
        self.key = key
        self.subject = discord.ui.TextInput(label='件名', max_length=150)
        self.content = discord.ui.TextInput(label='相談内容', style=discord.TextStyle.paragraph, max_length=3500)
        self.add_item(self.subject)
        self.add_item(self.content)
        self.party = None
        if DESKS[key].urgent:
            self.party = discord.ui.TextInput(label='問題の当事者（1人のIDまたはメンション）', required=False, max_length=80)
            self.add_item(self.party)

    async def on_submit(self, interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            channel = await interaction.client.create_ticket(interaction, self.key, self.subject.value, self.content.value,
                self.party.value if self.party else '')
            await interaction.followup.send(f'相談窓口を作成しました：{channel.mention}', ephemeral=True)
        except (TicketError, ValueError) as exc:
            await reply(interaction, str(exc))
        except Exception as exc:
            logger.error('Ticket creation failed: %s', type(exc).__name__)
            await reply(interaction, '作成できませんでした。管理者へBOT設定の確認を依頼してください。')


class DeskSelect(discord.ui.Select):
    def __init__(self):
        super().__init__(placeholder='相談窓口を選択', options=[discord.SelectOption(label=d.label, value=k) for k, d in DESKS.items()])

    async def callback(self, interaction):
        try:
            interaction.client.check_staff(interaction)
            await interaction.response.send_modal(RequestModal(self.values[0]))
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

    @discord.ui.button(label='チケットを閉じる', custom_id='concierge:close:v1', style=discord.ButtonStyle.danger)
    async def close_ticket(self, interaction, button):
        try:
            interaction.client.authorize_close(interaction)
            await interaction.response.send_message('本当に閉じますか？チャンネルは完全削除されます。',
                view=ConfirmClose(interaction.user.id), ephemeral=True)
        except TicketError as exc:
            await reply(interaction, str(exc))


class ConfirmClose(discord.ui.View):
    def __init__(self, requester):
        super().__init__(timeout=60)
        self.requester = requester

    @discord.ui.button(label='はい、削除する', style=discord.ButtonStyle.danger)
    async def confirm(self, interaction, button):
        if interaction.user.id != self.requester:
            return await reply(interaction, '確認操作を行った本人だけが実行できます。')
        await interaction.response.defer(ephemeral=True, thinking=True)
        bot = interaction.client
        async with bot.close_lock:
            try:
                row = bot.authorize_close(interaction)  # Revalidate after confirmation and lock.
                await bot.transcript(interaction.channel, row)
                await interaction.channel.delete(reason='Ticket close confirmed')
                bot.store.remove(interaction.channel_id)
                self.stop()
                await reply(interaction, 'チケットを削除しました。')
            except TicketError as exc:
                await reply(interaction, str(exc))
            except discord.HTTPException:
                await reply(interaction, '処理に失敗しました。チャンネルの状態・権限を確認してください。')

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


if __name__ == '__main__':
    bot.run(os.environ['DISCORD_TOKEN'], log_handler=None)
