import uuid
import random
from typing import List
from scheduler.implementation.node import Node
from scheduler.core.action import Action
from scheduler.core.node_response import NodeResponse


class SafraNode(Node):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        super().__init__(node_id, neighbors)
        self.color = "WHITE"
        self.is_active = False
        self.is_initiator = False
        self.pending_token = None
        self.total_nodes = 8

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data
        msg_type = data.get("type", data.get("message_type"))
        outbox = []

        if msg_type == "New":
            if not self.is_active and random.random() < 0.2:
                self.is_active = True
                target = random.choice(self.neighbors)
                outbox.append(self._msg(target, "APP", message.action_id))
                print(
                    f"[APP] Node {self.node_id} became ACTIVE and sent APP to {target}"
                )
            elif self.is_active and random.random() < 0.5:
                self.is_active = False
                print(f"[SAFRA] Node {self.node_id} became PASSIVE.")
                if self.pending_token:
                    outbox.extend(self._forward_token(message.action_id))

            if not self.is_initiator and random.random() < 0.1:
                self.is_initiator = True
                print(
                    f"[SAFRA] Node {self.node_id} is INITIATOR. Starting WHITE token."
                )
                outbox.extend(self._forward_token(message.action_id, init=True))

        elif msg_type == "APP":
            self.is_active = True
            self.color = "BLACK"
            print(f"[APP] Node {self.node_id} received APP. Turned BLACK and ACTIVE.")

        elif msg_type == "TOKEN":
            t_color = data.get("token_color")
            t_visited = set(data.get("visited", []))

            if self.is_initiator and len(t_visited) >= self.total_nodes:
                if t_color == "WHITE" and self.color == "WHITE":
                    print(
                        f"\n[TERMINATION] >>> Node {self.node_id} detected termination via SAFRA token! <<<\n"
                    )
                else:
                    print(
                        f"[SAFRA] Token returned BLACK or initiator BLACK. Restarting token."
                    )
                    self.color = "WHITE"
                    outbox.extend(self._forward_token(message.action_id, init=True))
            else:
                t_visited.add(self.node_id)
                self.pending_token = {"color": t_color, "visited": list(t_visited)}
                if not self.is_active:
                    outbox.extend(self._forward_token(message.action_id))

        return NodeResponse(outbox)

    def _forward_token(self, a_id, init=False):
        outbox = []
        if init:
            t_color = "WHITE"
            t_visited = [self.node_id]
        else:
            t_color = (
                "BLACK"
                if self.color == "BLACK" or self.pending_token["color"] == "BLACK"
                else "WHITE"
            )
            t_visited = self.pending_token["visited"]
            self.color = "WHITE"
            self.pending_token = None

        unvisited = [n for n in self.neighbors if n not in t_visited]
        target = unvisited[0] if unvisited else self.neighbors[0]

        outbox.append(
            Action(
                data={
                    "type": "TOKEN",
                    "sender_id": self.node_id,
                    "token_color": t_color,
                    "visited": t_visited,
                },
                node_id=target,
                action_id=a_id or uuid.uuid4(),
            )
        )
        print(f"[TOKEN] Node {self.node_id} forwarded {t_color} token to {target}.")
        return outbox

    def _msg(self, to_id, m_type, a_id):
        return Action(
            data={"type": m_type, "sender_id": self.node_id},
            node_id=to_id,
            action_id=a_id or uuid.uuid4(),
        )
