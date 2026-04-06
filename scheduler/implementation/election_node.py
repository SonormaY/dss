import uuid
from typing import List
from scheduler.implementation.node import Node
from scheduler.core.action import Action
from scheduler.core.node_response import NodeResponse


class ElectionNode(Node):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        super().__init__(node_id, neighbors)
        self.candidate = None
        self.parent = None
        self.expected_replies = 0
        self.received_replies = 0

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data
        msg_type = data.get("type", data.get("message_type"))
        sender_id = data.get("sender_id")
        c_id = data.get("candidate_id")
        outbox_actions = []

        if msg_type == "New":
            if self.candidate is None or str(self.node_id) > str(self.candidate):
                self.candidate = str(self.node_id)
                self.parent = self.node_id
                self.received_replies = 0
                self.expected_replies = len(self.neighbors)

                print(
                    f"[ELECTION] Node {self.node_id} STARTS election. Candidate: {self.candidate[:8]}"
                )

                for neighbor in self.neighbors:
                    outbox_actions.append(
                        self._create_msg(
                            neighbor, "EXPLORE", self.candidate, message.action_id
                        )
                    )

        elif msg_type == "EXPLORE":
            if self.candidate is None or str(c_id) > str(self.candidate):
                old_cand = str(self.candidate)[:8] if self.candidate else "None"
                print(
                    f"[EXTINCTION] Node {self.node_id} joins stronger wave {str(c_id)[:8]}. Abandons {old_cand}."
                )

                self.candidate = str(c_id)
                self.parent = sender_id
                self.received_replies = 0
                self.expected_replies = len(self.neighbors) - 1

                targets = [n for n in self.neighbors if n != self.parent]
                for neighbor in targets:
                    outbox_actions.append(
                        self._create_msg(
                            neighbor, "EXPLORE", self.candidate, message.action_id
                        )
                    )

                if self.expected_replies == 0:
                    print(
                        f"[ECHO] Node {self.node_id} is a leaf for wave {str(c_id)[:8]}. Returns ECHO to {self.parent}."
                    )
                    outbox_actions.append(
                        self._create_msg(
                            self.parent, "ECHO", self.candidate, message.action_id
                        )
                    )

            else:
                outbox_actions.append(
                    self._create_msg(sender_id, "ECHO", c_id, message.action_id)
                )

        elif msg_type == "ECHO":
            if str(c_id) == str(self.candidate):
                self.received_replies += 1

                if self.received_replies == self.expected_replies:
                    if self.parent == self.node_id:
                        print(
                            f"\n[LEADER] >>> Node {self.node_id} WON THE ELECTION! Wave {str(self.candidate)[:8]} covered the entire graph. <<<\n"
                        )
                    else:
                        print(
                            f"[ECHO] Node {self.node_id} collected all ECHOs for {str(self.candidate)[:8]}. Sending up to {self.parent}."
                        )
                        outbox_actions.append(
                            self._create_msg(
                                self.parent, "ECHO", self.candidate, message.action_id
                            )
                        )

        return NodeResponse(outbox_actions)

    def _create_msg(
        self, to_id: uuid.UUID, msg_type: str, c_id: str, action_id: uuid.UUID
    ) -> Action:
        return Action(
            data={"type": msg_type, "sender_id": self.node_id, "candidate_id": c_id},
            node_id=to_id,
            action_id=action_id or uuid.uuid4(),
        )
