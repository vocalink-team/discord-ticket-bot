"""Offline Discord API fixtures. Run: python -m unittest discover -s tests -v"""
import asyncio
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, patch

os.environ.update(GUILD_ID='100', TICKET_CATEGORY_ID='200', STAFF_ROLE_IDS='300',
                  TICKET_ENV='test', TICKET_DB_PATH=':memory:', TICKET_REMIND_MINUTES='60')
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bot as m
import discord

OWNER, PARTY, HANDLER, OTHER, BOT = 1001, 1002, 1003, 1004, 1999
CID = 9999


class WorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = m.TicketBot()
        self.bot._connection.user = discord.ClientUser(state=self.bot._connection,
            data={'id': str(BOT), 'username': 'testbot', 'discriminator': '0', 'avatar': None, 'bot': True})
        role_ids = {rid for d in m.DESKS.values() for rid in d.roles} | {100, 300}
        roles = [{'id': str(rid), 'name': str(rid), 'permissions': '0', 'position': i}
                 for i, rid in enumerate(sorted(role_ids))]
        self.guild = discord.Guild(state=self.bot._connection, data={
            'id': '100', 'name': 'test', 'owner_id': '8888', 'roles': roles})
        self.bot._connection._guilds[self.guild.id] = self.guild
        self.members = {}
        for uid, rids in [(OWNER, [300]), (PARTY, list(m.DESKS['report'].roles)),
                          (HANDLER, list(m.DESKS['report'].roles)+list(m.DESKS['development'].roles)),
                          (OTHER, [300]), (BOT, [])]:
            mem = discord.Member(guild=self.guild, state=self.bot._connection, data={
                'user': {'id': str(uid), 'username': str(uid), 'discriminator': '0', 'avatar': None, 'bot': uid == BOT},
                'roles': [str(r) for r in rids], 'joined_at': None, 'flags': 0})
            self.members[uid] = mem
            self.guild._add_member(mem)
        self.bot.store.add(CID, OWNER, 'report', PARTY)
        self.channel = discord.TextChannel(state=self.bot._connection, guild=self.guild,
            data={'id': str(CID), 'name': 'ticket-test', 'type': 0, 'position': 0, 'permission_overwrites': []})
        self.guild._channels[CID] = self.channel
        self.patches = []
        async def fetch_member(guild, uid): return self.members[uid]
        async def fetch_channel(guild, cid): return self.channel
        async def fetch_members(guild, **kwargs):
            for member in self.members.values(): yield member
        async def edit(channel, **kwargs):
            self.set_overwrites(kwargs['overwrites'])
            return self.channel
        self.start(patch.object(discord.Guild, 'fetch_member', fetch_member))
        self.start(patch.object(discord.Guild, 'fetch_channel', fetch_channel))
        self.start(patch.object(discord.Guild, 'fetch_members', fetch_members))
        self.start(patch.object(discord.TextChannel, 'edit', edit))
        self.send = self.start(patch.object(discord.TextChannel, 'send', AsyncMock()))
        self.delete = self.start(patch.object(discord.TextChannel, 'delete', AsyncMock()))
        info = self.bot.store.info(CID)
        overwrites = await self.bot.routing_overwrites(self.guild, info, self.target())
        self.set_overwrites(overwrites)

    def start(self, p):
        self.patches.append(p)
        return p.start()

    def set_overwrites(self, overwrites):
        data = []
        for target, overwrite in overwrites.items():
            allow, deny = overwrite.pair()
            data.append({'id': str(target.id), 'type': 0 if isinstance(target, discord.Role) else 1,
                         'allow': str(allow.value), 'deny': str(deny.value)})
        self.channel._fill_overwrites({'permission_overwrites': data})

    def target(self, **extra):
        return dict(status='waiting', desk='report', sensitive=1, assignee=None,
                    created_at=time.time(), notified_at=0, **extra)

    def interaction(self, uid=OWNER):
        return NS(guild=self.guild, guild_id=self.guild.id, channel=self.channel, channel_id=CID,
                  user=self.members[uid], client=self.bot,
                  response=NS(is_done=lambda: True, defer=AsyncMock(), send_modal=AsyncMock(),
                              send_message=AsyncMock(), edit_message=AsyncMock()),
                  followup=NS(send=AsyncMock()))

    async def asyncTearDown(self):
        for p in reversed(self.patches): p.stop()
        await self.bot.close()

    async def test_archive_read_only_reopen_exclusion_persists(self):
        await self.bot.change_archive(self.interaction(), True)
        self.assertEqual(self.bot.store.info(CID)['status'], 'archived')
        for uid in (HANDLER,):
            perms = self.channel.permissions_for(self.members[uid])
            self.assertTrue(perms.view_channel)
            self.assertFalse(perms.send_messages)
            self.assertFalse(perms.send_messages_in_threads)
        self.assertFalse(self.channel.permissions_for(self.members[PARTY]).view_channel)
        self.assertFalse(self.channel.permissions_for(self.members[OTHER]).view_channel)
        self.assertFalse(self.channel.permissions_for(self.members[OWNER]).view_channel)
        self.delete.assert_not_awaited()
        await self.bot.change_archive(self.interaction(HANDLER), False)
        self.assertEqual(self.bot.store.info(CID)['status'], 'waiting')
        self.assertTrue(self.channel.permissions_for(self.members[OWNER]).send_messages)
        self.assertFalse(self.channel.permissions_for(self.members[PARTY]).view_channel)

    async def test_concurrent_claim_and_release(self):
        await self.bot.claim(self.interaction(HANDLER))
        self.assertEqual(self.bot.store.info(CID)['assignee'], HANDLER)
        await self.bot.claim(self.interaction(HANDLER))
        self.assertEqual(self.bot.store.info(CID)['status'], 'waiting')
        self.assertIsNone(self.bot.store.info(CID)['assignee'])
        with self.assertRaises(m.TicketError): await self.bot.claim(self.interaction(OTHER))
        with self.assertRaises(m.TicketError): await self.bot.claim(self.interaction(PARTY))

    async def test_claim_race_has_one_winner(self):
        # A second eligible member is added after initial private routing.
        self.members[OTHER]._roles = discord.utils.SnowflakeList(m.DESKS['report'].roles)
        results = await asyncio.gather(self.bot.claim(self.interaction(HANDLER)),
                                       self.bot.claim(self.interaction(OTHER)), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, m.TicketError) for r in results), 1)
        self.assertIn(self.bot.store.info(CID)['assignee'], (HANDLER, OTHER))

    async def test_proposal_does_not_change_permissions_or_state(self):
        before = self.bot.store.info(CID)
        await self.bot.propose_transfer(self.interaction(HANDLER), 'development')
        self.assertEqual(before, self.bot.store.info(CID))
        self.assertTrue(self.send.call_args.kwargs['view'].owner == OWNER)
        self.assertIn('全履歴', self.send.call_args.args[0])

    async def test_transfer_requires_owner_and_current_revision(self):
        rev = self.bot.store.info(CID)['revision']
        with self.assertRaises(m.TicketError):
            await self.bot.approve_transfer(self.interaction(HANDLER), rev, 'development')
        self.bot.store.update(CID, status='active', assignee=HANDLER)
        with self.assertRaises(m.TicketError):
            await self.bot.approve_transfer(self.interaction(), rev, 'development')
        self.assertEqual(self.bot.store.info(CID)['desk'], 'report')

    async def test_transfer_keeps_sensitive_flag_and_exclusion(self):
        rev = self.bot.store.info(CID)['revision']
        await self.bot.approve_transfer(self.interaction(), rev, 'development')
        info = self.bot.store.info(CID)
        self.assertEqual(info['desk'], 'development')
        self.assertEqual(info['sensitive'], 1)
        self.assertEqual(info['excluded'], PARTY)
        self.assertFalse(self.channel.permissions_for(self.members[PARTY]).view_channel)
        self.assertTrue(self.channel.permissions_for(self.members[HANDLER]).view_channel)
        with patch.object(m, 'LOG_ID', 777):
            with patch.object(discord.Guild, 'fetch_channel', AsyncMock()) as fetch:
                await self.bot.transcript(self.channel, self.bot.store.get(CID))
                fetch.assert_not_awaited()

    async def test_failed_edit_is_recoverable_and_blocks_other_actions(self):
        with patch.object(discord.TextChannel, 'edit', AsyncMock(side_effect=OSError('offline'))):
            with self.assertRaises(OSError): await self.bot.change_archive(self.interaction(), True)
        info = self.bot.store.info(CID)
        self.assertEqual(info['status'], 'updating')
        self.assertEqual(json.loads(info['pending'])['status'], 'archived')
        with self.assertRaises(m.TicketError): await self.bot.claim(self.interaction(HANDLER))
        await self.bot.apply_transition(self.channel, info, json.loads(info['pending']))
        self.assertEqual(self.bot.store.info(CID)['status'], 'archived')
        self.assertIsNone(self.bot.store.info(CID)['pending'])

    async def test_reminder_private_recipients_and_throttle(self):
        self.bot.store.update(CID, created_at=1, notified_at=0)
        await self.bot.remind_once()
        self.send.assert_awaited_once()
        call = self.send.call_args
        users = call.kwargs['allowed_mentions'].users
        self.assertEqual([u.id for u in users], [HANDLER])
        self.assertFalse(call.kwargs['allowed_mentions'].roles)
        await self.bot.remind_once()
        self.send.assert_awaited_once()
        self.bot.store.update(CID, status='active', created_at=1, notified_at=0)
        await self.bot.remind_once()
        self.send.assert_awaited_once()

    async def test_archived_does_not_receive_reminder(self):
        self.bot.store.update(CID, status='archived', created_at=1, notified_at=0)
        await self.bot.remind_once()
        self.send.assert_not_awaited()

    async def test_excluded_administrator_blocks_permission_change(self):
        role = self.guild.get_role(m.DESKS['report'].roles[0])
        role._permissions |= discord.Permissions(administrator=True).value
        with self.assertRaises(m.TicketError): await self.bot.change_archive(self.interaction(), True)
        self.assertEqual(self.bot.store.info(CID)['status'], 'waiting')

    async def test_reopen_enforces_quota(self):
        self.bot.store.update(CID, status='archived')
        for n in range(m.MAX_OPEN): self.bot.store.add(5000+n, OWNER, 'development', None)
        with self.assertRaises(m.TicketError): await self.bot.change_archive(self.interaction(HANDLER), False)

    async def test_missing_destination_role_never_grants_access(self):
        self.guild._roles.pop(m.DESKS['development'].roles[0])
        with self.assertRaises(m.TicketError):
            await self.bot.approve_transfer(self.interaction(), 0, 'development')
        self.assertEqual(self.bot.store.info(CID)['desk'], 'report')

    async def test_delete_requires_archived_and_does_not_delete_after_reopen(self):
        rev = self.bot.store.info(CID)['revision']
        confirm = m.ConfirmDelete(HANDLER, rev)
        await confirm.confirm.callback(self.interaction(HANDLER))
        self.delete.assert_not_awaited()
        await self.bot.change_archive(self.interaction(), True)
        confirm = m.ConfirmDelete(HANDLER, self.bot.store.info(CID)['revision'])
        await self.bot.change_archive(self.interaction(HANDLER), False)
        await confirm.confirm.callback(self.interaction(HANDLER))
        self.delete.assert_not_awaited()

    async def test_delete_archived_sensitive_never_exports(self):
        await self.bot.change_archive(self.interaction(), True)
        confirm = m.ConfirmDelete(HANDLER, self.bot.store.info(CID)['revision'])
        await confirm.confirm.callback(self.interaction(HANDLER))
        self.delete.assert_awaited_once()
        self.assertIsNone(self.bot.store.info(CID))

    async def test_persistent_controls_and_picker(self):
        self.assertTrue(m.CloseView().is_persistent())
        self.assertEqual(len(m.CloseView().children), 5)
        self.assertEqual(len(m.RequestModal('report', PARTY).children), 2)
        view = m.PartyView(OWNER)
        self.assertFalse(await view.interaction_check(self.interaction(OTHER)))
        approval = m.TransferApproval(OWNER, 0, 'development')
        self.assertFalse(await approval.interaction_check(self.interaction(HANDLER)))
        self.assertTrue(await approval.interaction_check(self.interaction()))

    async def test_log_failure_prevents_permanent_deletion(self):
        self.bot.store.update(CID, desk='development', sensitive=0)
        await self.bot.change_archive(self.interaction(), True)
        confirm = m.ConfirmDelete(HANDLER, self.bot.store.info(CID)['revision'])
        with patch.object(self.bot, 'transcript', AsyncMock(side_effect=m.TicketError('log unavailable'))):
            await confirm.confirm.callback(self.interaction(HANDLER))
        self.delete.assert_not_awaited()
        self.assertIsNotNone(self.bot.store.info(CID))

    async def test_old_department_loses_access_after_transfer(self):
        # Handler belongs only to the source department in this scenario.
        self.members[HANDLER]._roles = discord.utils.SnowflakeList(m.DESKS['report'].roles)
        # Another account supplies a destination handler.
        self.members[OTHER]._roles = discord.utils.SnowflakeList(m.DESKS['development'].roles)
        await self.bot.approve_transfer(self.interaction(), 0, 'development')
        self.assertFalse(self.channel.permissions_for(self.members[HANDLER]).view_channel)
        self.assertTrue(self.channel.permissions_for(self.members[OTHER]).view_channel)
        with self.assertRaises(m.TicketError):
            await self.bot.claim(self.interaction(HANDLER))

    async def test_departed_party_still_has_member_deny(self):
        async def fetch(guild, uid):
            if uid == PARTY:
                raise discord.NotFound(NS(status=404, reason='Not Found'), {'code': 10007, 'message': 'Unknown Member'})
            return self.members[uid]
        with patch.object(discord.Guild, 'fetch_member', fetch):
            await self.bot.change_archive(self.interaction(), True)
        # The account can rejoin and receive a handler role without gaining access.
        self.assertFalse(self.channel.permissions_for(self.members[PARTY]).view_channel)

    async def test_decline_changes_no_access(self):
        view = m.TransferApproval(OWNER, 0, 'development')
        before = self.bot.store.info(CID)
        await view.reject.callback(self.interaction())
        self.assertEqual(before, self.bot.store.info(CID))
        self.assertFalse(await view.interaction_check(self.interaction()))

    async def test_wrong_guild_rejected(self):
        i = self.interaction(); i.guild_id = 999
        with self.assertRaises(m.TicketError): await self.bot.fresh_ticket(i)

    async def test_archived_reporter_with_handler_role_is_denied(self):
        self.members[OWNER]._roles = discord.utils.SnowflakeList(m.DESKS['report'].roles)
        await self.bot.change_archive(self.interaction(HANDLER), True)
        self.assertFalse(self.channel.permissions_for(self.members[OWNER]).view_channel)
        self.assertTrue(self.channel.permissions_for(self.members[HANDLER]).view_channel)
        with self.assertRaises(m.TicketError):
            await self.bot.change_archive(self.interaction(), False)
        with self.assertRaises(m.TicketError):
            await self.bot.fresh_ticket(self.interaction())
        await self.bot.change_archive(self.interaction(HANDLER), False)
        self.assertTrue(self.channel.permissions_for(self.members[OWNER]).view_channel)
        self.assertFalse(self.channel.permissions_for(self.members[PARTY]).view_channel)

    async def test_administrator_reporter_cannot_be_marked_hidden(self):
        self.members[OWNER]._roles = discord.utils.SnowflakeList([300])
        self.guild.get_role(300)._permissions |= discord.Permissions(administrator=True).value
        with self.assertRaises(m.TicketError):
            await self.bot.change_archive(self.interaction(HANDLER), True)
        self.assertEqual(self.bot.store.info(CID)['status'], 'waiting')

    async def test_server_owner_reporter_cannot_be_marked_hidden(self):
        self.guild.owner_id = OWNER
        with self.assertRaises(m.TicketError):
            await self.bot.change_archive(self.interaction(HANDLER), True)
        self.assertEqual(self.bot.store.info(CID)['status'], 'waiting')

    async def test_settings_check_not_available_to_staff(self):
        with self.assertRaises(m.TicketError): await self.bot.config_check(self.interaction(OTHER))


class MigrationTests(unittest.TestCase):
    def test_old_database_migrates_without_loss_and_can_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'tickets.sqlite3')
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE tickets (channel INTEGER PRIMARY KEY, owner INTEGER NOT NULL, desk TEXT NOT NULL, excluded INTEGER)')
            db.execute('INSERT INTO tickets VALUES (?,?,?,?)', (CID, OWNER, 'report', PARTY))
            db.commit(); db.close()
            with patch.object(m, 'DB_PATH', path):
                store = m.Store()
                self.assertEqual(store.get(CID), (OWNER, 'report', PARTY))
                self.assertEqual(store.info(CID)['sensitive'], 1)
                store.update(CID, status='archived')
                store.db.close()
                store = m.Store()
                self.assertEqual(store.info(CID)['status'], 'archived')
                self.assertEqual(store.info(CID)['excluded'], PARTY)
                store.db.close()


def tearDownModule():
    m.bot.store.db.close()


if __name__ == '__main__':
    unittest.main()
