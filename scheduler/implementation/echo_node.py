import uuid
from typing import List
from scheduler.implementation.node import Node
from scheduler.core.action import Action
from scheduler.core.node_response import NodeResponse


class EchoNode(Node):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        super().__init__(node_id, neighbors)
        self.parent = None
        self.received_count = 0
        self.is_initiator = False

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data
        msg_type = data.get("type", data.get("message_type"))
        sender_id = data.get("sender_id")
        outbox_actions = []

        if msg_type == "New":
            if not self.is_initiator and self.parent is None:
                self.is_initiator = True
                print(
                    f"[ECHO] Node {self.node_id} is INITIATOR. Sending EXPLORE to all neighbors."
                )
                for neighbor in self.neighbors:
                    outbox_actions.append(
                        self._create_msg(neighbor, "EXPLORE", message.action_id)
                    )

        elif msg_type in ["EXPLORE", "ECHO"]:
            if msg_type == "EXPLORE" and self.parent is None and not self.is_initiator:
                self.parent = sender_id
                print(f"[ECHO] Node {self.node_id} accepted {sender_id} as Parent.")
                for neighbor in self.neighbors:
                    if neighbor != self.parent:
                        outbox_actions.append(
                            self._create_msg(neighbor, "EXPLORE", message.action_id)
                        )

            self.received_count += 1

            if self.received_count == len(self.neighbors):
                if self.is_initiator:
                    print(
                        f"\n[ECHO] >>> Node {self.node_id} (Initiator) received all replies. ALGORITHM DECIDED! <<<\n"
                    )
                else:
                    print(
                        f"[ECHO] Node {self.node_id} got all replies. Sending ECHO back to Parent {self.parent}."
                    )
                    outbox_actions.append(
                        self._create_msg(self.parent, "ECHO", message.action_id)
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
