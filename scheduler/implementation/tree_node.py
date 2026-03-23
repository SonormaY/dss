import uuid
from typing import List
from scheduler.implementation.node import Node
from scheduler.core.action import Action
from scheduler.core.node_response import NodeResponse


class TreeNode(Node):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        super().__init__(node_id, neighbors)
        self.received_from = set()
        self.has_sent = False

        if len(self.neighbors) == 1:
            self.mailbox.add_inbox_action(
                Action(
                    data={"type": "WAKEUP"},
                    node_id=self.node_id,
                    action_id=uuid.uuid4(),
                )
            )

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data
        msg_type = data.get("type", data.get("message_type"))
        sender_id = data.get("sender_id")
        outbox_actions = []

        if msg_type == "WAKEUP" and not self.has_sent:
            neighbor = self.neighbors[0]
            print(
                f"[TREE] Leaf Node {self.node_id} initiates wave. Sending TOKEN to {neighbor}."
            )
            outbox_actions.append(
                self._create_msg(neighbor, "TOKEN", message.action_id)
            )
            self.has_sent = True

        elif msg_type == "TOKEN":
            if sender_id:
                self.received_from.add(sender_id)
                print(f"[TREE] Node {self.node_id} received TOKEN from {sender_id}.")

            unreceived = set(self.neighbors) - self.received_from

            if len(unreceived) == 1 and not self.has_sent:
                target = list(unreceived)[0]
                print(
                    f"[TREE] Node {self.node_id} received from all but {target}. Sending TOKEN to {target}."
                )
                outbox_actions.append(
                    self._create_msg(target, "TOKEN", message.action_id)
                )
                self.has_sent = True

            elif len(unreceived) == 0:
                print(
                    f"\n[TREE] >>> Node {self.node_id} received TOKEN from ALL neighbors. ALGORITHM DECIDED! <<<\n"
                )

        return NodeResponse(outbox_actions)

    def _create_msg(
        self, to_id: uuid.UUID, msg_type: str, action_id: uuid.UUID
    ) -> Action:
        return Action(
            data={"type": msg_type, "sender_id": self.node_id},
            node_id=to_id,
            action_id=action_id or uuid.uuid4(),
        )
