import uuid
from typing import List
from scheduler.implementation.node import Node
from scheduler.core.action import Action
from scheduler.core.node_response import NodeResponse


class AwerbuchNode(Node):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        super().__init__(node_id, neighbors)
        self.parent = None
        self.children = []

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data
        msg_type = data.get("type", data.get("message_type"))
        sender_id = data.get("sender_id")
        outbox_actions = []

        if msg_type == "New":
            if self.parent is None:
                self.parent = self.node_id
                for neighbor in self.neighbors:
                    outbox_actions.append(
                        self._create_msg(neighbor, "CONNECT", message.action_id)
                    )

        elif msg_type == "CONNECT":
            if self.parent is None:
                self.parent = sender_id
                outbox_actions.append(
                    self._create_msg(sender_id, "ACCEPT", message.action_id)
                )

                targets = [n for n in self.neighbors if n != sender_id]
                for neighbor in targets:
                    outbox_actions.append(
                        self._create_msg(neighbor, "CONNECT", message.action_id)
                    )
            else:
                outbox_actions.append(
                    self._create_msg(sender_id, "REJECT", message.action_id)
                )

        elif msg_type == "ACCEPT":
            self.children.append(sender_id)

        elif msg_type == "REJECT":
            pass

        return NodeResponse(outbox_actions)

    def _create_msg(
        self, to_id: uuid.UUID, msg_type: str, action_id: uuid.UUID
    ) -> Action:
        return Action(
            data={"type": msg_type, "sender_id": self.node_id},
            node_id=to_id,
            action_id=action_id or uuid.uuid4(),
        )
