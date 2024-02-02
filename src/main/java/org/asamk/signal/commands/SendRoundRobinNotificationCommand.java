package org.asamk.signal.commands;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.asamk.signal.commands.exceptions.CommandException;
import org.asamk.signal.commands.exceptions.UnexpectedErrorException;
import org.asamk.signal.commands.exceptions.UserErrorException;
import org.asamk.signal.manager.Manager;
import org.asamk.signal.manager.api.AlreadyReceivingException;
import org.asamk.signal.manager.api.AttachmentInvalidException;
import org.asamk.signal.manager.api.Group;
import org.asamk.signal.manager.api.InvalidNumberException;
import org.asamk.signal.manager.api.InvalidStickerException;
import org.asamk.signal.manager.api.Message;
import org.asamk.signal.manager.api.MessageEnvelope;
import org.asamk.signal.manager.api.RecipientAddress;
import org.asamk.signal.manager.api.RecipientIdentifier;
import org.asamk.signal.manager.api.UnregisteredRecipientException;
import org.asamk.signal.manager.api.GroupNotFoundException;
import org.asamk.signal.manager.api.GroupSendingNotAllowedException;
import org.asamk.signal.manager.api.NotAGroupMemberException;
import org.asamk.signal.manager.api.TextStyle;
import org.asamk.signal.output.OutputWriter;
import org.asamk.signal.output.PlainTextWriter;
import org.asamk.signal.util.CommandUtil;
import org.asamk.signal.util.SendMessageResultUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class SendRoundRobinNotificationCommand implements JsonRpcLocalCommand {

    private static final String IGNORE_AND_CONT_CMD = "ignorieren und neu ziehen";
    private static final String CHOOSE_ANOTHER_CMD = "neu ziehen";
    private static final String UNDO_CMD = "heute nicht";
    private static final String MENTION = "@mention";
    private static final String BOT_ADDRESS = "lieber bot:";
    private static final String YOUR_BOT = " -- Euer Essensverteiler-Bot";

    @Override
    public String getName() {
        return "sendRoundRobinNotification";
    }

    @Override
    public void attachToSubparser(final Subparser subparser) {
        subparser.help(
                "Send a notification to a group, mentioning a member of the group in round robin fashion. " +
                "The list of members is stored in the config, and the command will send to the next member " +
                "not already mentioned before and then restart from the top.");
        subparser.addArgument("-g", "--group-id", "--group").help("Specify the recipient group ID.");
        subparser.addArgument("-m", "--message").help("Specify message to send. Include @mention somewhere to mention the next user.");
        subparser.addArgument("-s", "--stay").type(int.class).help("Stay and watch for commands for N receive cycles of a few seconds each.");
        subparser.addArgument("-i", "--ignore-before").setDefault(5).type(int.class).help("Ignore commands before hour N of the current day (to avoid commands that came too late for the last turn to affect this one.");
    }

    @Override
    public void handleCommand(
            final Namespace ns, final Manager m, final OutputWriter outputWriter
    ) throws CommandException {
        // handle argument errors
        final var groupIdString = ns.getString("group-id");
        final var messageText = ns.getString("message");
        if (groupIdString == null) {
            throw new UserErrorException("No group ID given");
        }
        if (messageText == null || !messageText.contains(MENTION)) {
            throw new UserErrorException("Message text must contain the literal string '" + MENTION + "'");
        }
        int receiveCycles;
        try {
            receiveCycles = ns.getInt("stay");
        } catch (NullPointerException e) {
            throw new UserErrorException("Number of receive cycles must be a valid integer");
        }

        int ignoreBefore;
        try {
            ignoreBefore = ns.getInt("ignore-before");
        } catch (NullPointerException e) {
            System.err.println("ignore-before option must be a valid integer, defaulting to 5");
            ignoreBefore = 5;
        }

        // get the group info we need
        var gid = CommandUtil.getGroupId(groupIdString);
        var groups = m.getGroups();
        groups = groups.stream().filter(g -> gid.equals(g.groupId())).toList();
        if (groups.size() != 1) {
            throw new UserErrorException("No group found for gid");
        }
        var group = groups.get(0);
        if (outputWriter instanceof PlainTextWriter ptWriter) {
            ptWriter.println(String.format("Round-robin notification system for %s", group.title()));
        }

        // read log
        JsonRoundRobinSendLog log = readLog(group);
        System.err.println("readLog");

        doRoundAndSendNotification(m, outputWriter, messageText, group, log);
        while (receiveCycles > 0) {
            try {
                handleCommands(m, outputWriter, messageText, group, log, ignoreBefore);
            } catch (InvalidNumberException e1) {
                // shouldn't happen at all
                e1.printStackTrace();
            }
            receiveCycles--;
            System.err.println("Doing another " + receiveCycles + "cycles");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void doRoundAndSendNotification(final Manager m, final OutputWriter outputWriter, final String messageText, Group group,
            JsonRoundRobinSendLog log) throws UnexpectedErrorException, CommandException, UserErrorException {
        // decide who's next
        var availJsonMembers = resolveJsonMembers(group.members());
        availJsonMembers.removeAll(log.ignoredMembers);
        availJsonMembers.removeAll(log.servedMembers);

        RecipientAddress nextRecipient;
        var recipientAddressList = new ArrayList<>(group.members());
        Collections.shuffle(recipientAddressList);
        if (availJsonMembers.isEmpty()) {
            log.servedMembers.clear();
            nextRecipient = recipientAddressList.get(0);
        } else {
            nextRecipient = recipientAddressList.stream()
                .filter(member -> availJsonMembers.stream().anyMatch(jsonMember -> resolveJsonMember(member).equals(jsonMember)))
                .toList()
                .get(0);
        }
        System.err.println("nextRecp");

        // update log
        var jsonNextRecipient = resolveJsonMember(nextRecipient);
        assert !log.servedMembers.contains(jsonNextRecipient) : "ouch, recipient duplicated";
        log.servedMembers.add(jsonNextRecipient);
        writeLog(group, log);
        System.err.println("logwritten");

        // send it
        var mentions = List.of(new Message.Mention(RecipientIdentifier.Single.fromAddress(nextRecipient),
                messageText.indexOf(MENTION),
                MENTION.length()));
        sendSimpleMessage(m, outputWriter, group, messageText, mentions);
    }

    private void handleCommands(final Manager m, final OutputWriter outputWriter, String messageText, Group group, JsonRoundRobinSendLog log, int ignoreBefore)
            throws UnexpectedErrorException, CommandException, UserErrorException, InvalidNumberException {
        // receive some messages, to make signal API endpoints happy and react to commands
        System.err.println("receiving...");
        final var messages = new ArrayList<MessageEnvelope>();
        try {
            m.receiveMessages(Optional.ofNullable(Duration.ofMillis(3000)), Optional.ofNullable(-1), new Manager.ReceiveMessageHandler() {
                @Override
                public void handleMessage(MessageEnvelope envelope, Throwable e) {
                    messages.add(envelope);
                }
            });
        } catch (IOException | AlreadyReceivingException e) {
            throw new UnexpectedErrorException("Failed to send message: " + e.getMessage() + " (" + e.getClass().getSimpleName() + ")", e);
        }
        System.err.println("gotMessages " + messages.size());

        // handle commands
        for (var message : messages) {
            var data = message.data();
            if (data.isPresent()) {
                if (data.get().groupContext().isPresent()) {
                    if (!data.get().groupContext().get().groupId().equals(group.groupId())) {
                        continue;
                    }
                } else {
                    continue;
                }
                System.err.println("got data from right chat " + data.toString());
                if (message.serverDeliveredTimestamp() < LocalDateTime.now().withHour(ignoreBefore).withMinute(0)
                        .toEpochSecond(ZoneOffset.UTC) * 1000) {
                    // skip commands from before today
                    continue;
                }
                System.err.println("got data from right time " + message.serverDeliveredTimestamp());
                var body = data.get().body();
                if (body.isPresent()) {
                    var txt = body.get().toLowerCase();
                    System.err.println("got txt " + txt);
                    if (txt.startsWith(BOT_ADDRESS)) {
                        var cmd = txt.substring(BOT_ADDRESS.length()).strip();
                        System.err.println("got cmd " + cmd);
                        if (cmd.equals("help")) {
                            sendSimpleMessage(m, outputWriter, group, "Ich reagiere auf folgende Kommandos. " +
                                            "'" + UNDO_CMD + "' - mach den letzten zug rückängig. " +
                                            "'" + CHOOSE_ANOTHER_CMD + "' - mach den letzten zug rückängig und ziehe neu. " +
                                            "'" + IGNORE_AND_CONT_CMD + "' - den gezogenen zukünftig nie mehr ziehen und für heute neu ziehen. " +
                                            "'heute X, nicht Y' - wenn Y gezogen wurde, stattdessen X nehmen und Y zurück in den pool legen. ", null);
                        } else if (cmd.equals(UNDO_CMD)) {
                            if (log.servedMembers.size() > 0) {
                                var undoneMember = log.servedMembers.remove(log.servedMembers.size() - 1);
                                var msg = "Ok, heute nicht, habe {} wieder in den Pool genommen";
                                var mentions = List.of(new Message.Mention(RecipientIdentifier.Single.fromString(undoneMember.uuid, null),
                                                                msg.indexOf("{"), 2));
                                sendSimpleMessage(m, outputWriter, group, msg, mentions);
                                writeLog(group, log);
                            }
                        } else if (cmd.equals(CHOOSE_ANOTHER_CMD)) {
                            JsonGroupMember undoneMember = null;
                            if (log.servedMembers.size() > 0) {
                                undoneMember = log.servedMembers.get(log.servedMembers.size() - 1);
                            }
                            doRoundAndSendNotification(m, outputWriter, messageText, group, log);
                            if (undoneMember != null) {
                                var msg = "Habe {} wieder in den Pool genommen";
                                var mentions = List.of(new Message.Mention(RecipientIdentifier.Single.fromString(undoneMember.uuid, null),
                                                                msg.indexOf("{"), 2));
                                sendSimpleMessage(m, outputWriter, group, msg, mentions);
                                writeLog(group, log);
                            }
                        } else if (cmd.equals(IGNORE_AND_CONT_CMD)) {
                            if (log.servedMembers.size() > 0) {
                                var undoneMember = log.servedMembers.remove(log.servedMembers.size() - 1);
                                log.ignoredMembers.add(undoneMember);
                                var msg = "Ok, werde {} fortan ignorieren.";
                                var mentions = List.of(new Message.Mention(
                                        RecipientIdentifier.Single.fromString(undoneMember.uuid, null),
                                        msg.indexOf("{"), 2));
                                sendSimpleMessage(m, outputWriter, group, msg, mentions);
                                writeLog(group, log);
                            }
                            doRoundAndSendNotification(m, outputWriter, messageText, group, log);
                        } else if (cmd.contains("heute")
                                        && (cmd.contains(", nicht") || cmd.contains(",nicht"))
                                        && message.data().get().mentions().size() == 2) {
                            var mentions = message.data().get().mentions();
                            var firstMention = resolveJsonMember((mentions.get(0).start() < mentions.get(1).start() ?
                                                            mentions.get(0) : mentions.get(1)).recipient());
                            var secondMention = resolveJsonMember((mentions.get(0).start() > mentions.get(1).start() ?
                                                            mentions.get(0) : mentions.get(1)).recipient());
                            log.servedMembers.remove(firstMention);
                            if (!log.servedMembers.contains(secondMention)) {
                                log.servedMembers.add(secondMention);
                            }
                            var msg = "Habe {} wieder in den Pool genommen und [] vorerst als bedient markiert";
                            sendSimpleMessage(m, outputWriter, group, msg,
                                            List.of(
                                                    new Message.Mention(RecipientIdentifier.Single.fromString(firstMention.uuid, null),
                                                                    msg.indexOf("{"), 2),
                                                    new Message.Mention(RecipientIdentifier.Single.fromString(secondMention.uuid, null),
                                                                    msg.indexOf("["), 2)));
                            writeLog(group, log);
                        }
                    }
                }
            }
        }
        System.err.println("parsedMessages");
    }

    private void sendSimpleMessage(final Manager m, final OutputWriter outputWriter, Group group, String msg, List<Message.Mention> mentions)
            throws CommandException, UnexpectedErrorException, UserErrorException {
        try {
            if (mentions == null) {
                mentions = List.<Message.Mention>of();
            }
            var results = m.sendMessage(
                    new Message(
                            msg + YOUR_BOT,
                            List.<String>of(),
                            mentions,
                            Optional.<Message.Quote>ofNullable(null),
                            Optional.<Message.Sticker>ofNullable(null),
                            List.<Message.Preview>of(),
                            Optional.<Message.StoryReply>ofNullable(null),
                            List.<TextStyle>of()),
                    Set.of(new RecipientIdentifier.Group(group.groupId())),
                    true /* notifySelf */);
            SendMessageResultUtils.outputResult(outputWriter, results);
        } catch (AttachmentInvalidException | IOException e) {
            throw new UnexpectedErrorException("Failed to send message: " + e.getMessage()
                    + " (" + e.getClass().getSimpleName() + ")", e);
        } catch (GroupNotFoundException | NotAGroupMemberException
                | GroupSendingNotAllowedException e) {
            throw new UserErrorException(e.getMessage());
        } catch (UnregisteredRecipientException e) {
            throw new UserErrorException(
                    "The user " + e.getSender().getIdentifier() + " is not registered.");
        } catch (InvalidStickerException e) {
            throw new UserErrorException("Failed to send sticker: " + e.getMessage(), e);
        }
    }

    private void writeLog(Group group, JsonRoundRobinSendLog log) throws UnexpectedErrorException {
        var objectMapper = new ObjectMapper();
        var configFileName = getConfigFileName(group);
        try {
            objectMapper.writeValue(new File(configFileName), log);
        } catch (IOException e) {
            throw new UnexpectedErrorException("Error reading/writing log file", e);
        }
    }

    private JsonRoundRobinSendLog readLog(Group group) throws UnexpectedErrorException {
        var objectMapper = new ObjectMapper();
        var configFileName = getConfigFileName(group);
        try {
            if (Files.exists(Path.of(configFileName))) {
                return objectMapper.readValue(new File(configFileName), JsonRoundRobinSendLog.class);
            } else {
                var log = new JsonRoundRobinSendLog(List.of(), List.of());
                objectMapper.writeValue(new File(configFileName), log);
                return log;
            }
        } catch (IOException e) {
            throw new UnexpectedErrorException("Error reading/writing log file", e);
        }
    }

    private String getConfigFileName(Group group) {
        return String.format("round-robin-%s.json", group.groupId().toBase64());
    }

    private static Set<JsonGroupMember> resolveJsonMembers(Set<RecipientAddress> addresses) {
        return addresses.stream().map(address -> resolveJsonMember(address)).collect(Collectors.toSet());
    }

    private static JsonGroupMember resolveJsonMember(RecipientAddress address) {
        return new JsonGroupMember(address.number().orElse(null), address.uuid().map(UUID::toString).orElse(null));
    }

    record JsonRoundRobinSendLog(
            List<JsonGroupMember> ignoredMembers,
            List<JsonGroupMember> servedMembers) {}

    private record JsonGroupMember(String number, String uuid) {}
}
