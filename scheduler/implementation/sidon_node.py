import uuid
from typing import List
from scheduler.implementation.node import Node
from scheduler.core.action import Action
from scheduler.core.node_response import NodeResponse


class SidonNode(Node):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        super().__init__(node_id, neighbors)
        self.unvisited_neighbors = set(neighbors)
        self.parent = None
        self.is_visited = False

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data
        msg_type = data.get("type", data.get("message_type"))
        sender_id = data.get("sender_id")
        outbox_actions = []

        if msg_type == "New":
            if not self.is_visited:
                self.is_visited = True
                self.parent = self.node_id
                self._forward_token(outbox_actions, message.action_id)

        elif msg_type == "TOKEN":
            print(f"[WALK] Node {self.node_id} received token from {sender_id}")
            if sender_id and sender_id in self.unvisited_neighbors:
                self.unvisited_neighbors.remove(sender_id)

            if not self.is_visited:
                self.is_visited = True
                self.parent = sender_id

            self._forward_token(outbox_actions, message.action_id)

        elif msg_type == "RETURN":
            print(
                f"[WALK] Node {self.node_id} got token back (backtracking) from {sender_id}"
            )
            if sender_id and sender_id in self.unvisited_neighbors:
                self.unvisited_neighbors.remove(sender_id)
            self._forward_token(outbox_actions, message.action_id)

        return NodeResponse(outbox_actions)

    def _forward_token(self, outbox_actions: List[Action], action_id: uuid.UUID):
        if self.unvisited_neighbors:
            next_neighbor = self.unvisited_neighbors.pop()
            outbox_actions.append(self._create_msg(next_neighbor, "TOKEN", action_id))
        elif self.parent != self.node_id and self.parent is not None:
            outbox_actions.append(self._create_msg(self.parent, "RETURN", action_id))

    def _create_msg(
        self, to_id: uuid.UUID, msg_type: str, action_id: uuid.UUID
    ) -> Action:
        return Action(
            data={"type": msg_type, "sender_id": self.node_id},
            node_id=to_id,
            action_id=action_id or uuid.uuid4(),
        )
