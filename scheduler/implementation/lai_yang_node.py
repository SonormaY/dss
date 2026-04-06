import uuid
import random
from typing import List
from scheduler.implementation.node import Node
from scheduler.core.action import Action
from scheduler.core.node_response import NodeResponse


class LaiYangNode(Node):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        super().__init__(node_id, neighbors)
        self.color = "WHITE"
        self.state_counter = 0
        self.saved_state = None
        self.transit_messages = []

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data
        msg_type = data.get("type", data.get("message_type"))
        msg_color = data.get("color", "WHITE")
        sender_id = data.get("sender_id")
        outbox_actions = []

        trigger_snapshot = False

        if msg_type == "New":
            if self.color == "WHITE" and random.random() < 0.2:
                trigger_snapshot = True
            else:
                msg_type = "APP"

        if self.color == "WHITE" and (msg_color == "RED" or trigger_snapshot):
            self.color = "RED"
            self.saved_state = self.state_counter
            print(
                f"\n[SNAPSHOT] Node {self.node_id} turned RED. Saved state: {self.saved_state}\n"
            )

            for neighbor in self.neighbors:
                outbox_actions.append(
                    self._create_msg(neighbor, "MARKER", "RED", message.action_id)
                )

        if self.color == "RED" and msg_color == "WHITE" and msg_type == "APP":
            self.transit_messages.append(data)
            print(
                f"[TRANSIT] Node {self.node_id} intercepted a transit (WHITE) message from {sender_id}"
            )

        if msg_type == "APP":
            self.state_counter += 1
            target = random.choice(self.neighbors)
            outbox_actions.append(
                self._create_msg(target, "APP", self.color, message.action_id)
            )
            print(
                f"[APP] Node {self.node_id} ({self.color}) sent a message to {target}"
            )

        return NodeResponse(outbox_actions)

    def _create_msg(
        self, to_id: uuid.UUID, msg_type: str, color: str, action_id: uuid.UUID
    ) -> Action:
        return Action(
            data={"type": msg_type, "sender_id": self.node_id, "color": color},
            node_id=to_id,
            action_id=action_id or uuid.uuid4(),
        )
