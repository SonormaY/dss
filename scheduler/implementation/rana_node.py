import uuid
import random
from typing import List
from scheduler.implementation.node import Node
from scheduler.core.action import Action
from scheduler.core.node_response import NodeResponse


class RanaNode(Node):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        super().__init__(node_id, neighbors)
        self.clock = 0
        self.is_active = False
        self.unacked = 0
        self.waves = {}

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data
        msg_type = data.get("type", data.get("message_type"))
        sender_id = data.get("sender_id")
        outbox = []

        if msg_type == "New":
            if random.random() < 0.3 and not self.is_active:
                self.is_active = True
                target = random.choice(self.neighbors)
                self.clock += 1
                self.unacked += 1
                outbox.append(self._msg(target, "APP", self.clock, message.action_id))
                print(
                    f"[APP] Node {self.node_id} became ACTIVE and sent APP to {target}"
                )
            elif not self.is_active and self.unacked == 0:
                w_id = str(uuid.uuid4())[:8]
                self.waves[w_id] = {
                    "parent": self.node_id,
                    "replies": 0,
                    "expected": len(self.neighbors),
                    "clock": self.clock,
                }
                print(
                    f"[RANA] Node {self.node_id} is QUIET. Initiating wave {w_id} with clock {self.clock}"
                )
                for n in self.neighbors:
                    outbox.append(
                        self._wave_msg(
                            n, "EXPLORE", w_id, self.clock, message.action_id
                        )
                    )
            else:
                self.is_active = False
                print(f"[RANA] Node {self.node_id} became PASSIVE.")

        elif msg_type == "APP":
            self.clock = max(self.clock, data.get("clock", 0)) + 1
            self.is_active = True
            outbox.append(self._msg(sender_id, "ACK", self.clock, message.action_id))
            print(f"[APP] Node {self.node_id} received APP. Became ACTIVE. Sent ACK.")

        elif msg_type == "ACK":
            self.unacked = max(0, self.unacked - 1)

        elif msg_type == "EXPLORE":
            w_id = data.get("w_id")
            w_clock = data.get("clock")
            if self.is_active or self.unacked > 0:
                outbox.append(
                    self._wave_msg(
                        sender_id, "REJECT", w_id, self.clock, message.action_id
                    )
                )
            else:
                self.waves[w_id] = {
                    "parent": sender_id,
                    "replies": 0,
                    "expected": len(self.neighbors) - 1,
                    "clock": w_clock,
                }
                targets = [n for n in self.neighbors if n != sender_id]
                if not targets:
                    outbox.append(
                        self._wave_msg(
                            sender_id, "ECHO", w_id, self.clock, message.action_id
                        )
                    )
                else:
                    for n in targets:
                        outbox.append(
                            self._wave_msg(
                                n, "EXPLORE", w_id, self.clock, message.action_id
                            )
                        )

        elif msg_type in ["ECHO", "REJECT"]:
            w_id = data.get("w_id")
            if w_id in self.waves:
                if msg_type == "REJECT":
                    self.waves[w_id]["expected"] = -1
                    if self.waves[w_id]["parent"] != self.node_id:
                        outbox.append(
                            self._wave_msg(
                                self.waves[w_id]["parent"],
                                "REJECT",
                                w_id,
                                self.clock,
                                message.action_id,
                            )
                        )
                    else:
                        print(f"[RANA] Wave {w_id} failed. Someone is active.")
                else:
                    if self.waves[w_id]["expected"] != -1:
                        self.waves[w_id]["replies"] += 1
                        if self.waves[w_id]["replies"] == self.waves[w_id]["expected"]:
                            if self.waves[w_id]["parent"] == self.node_id:
                                print(
                                    f"\n[TERMINATION] >>> Node {self.node_id} detected termination via wave {w_id}! <<<\n"
                                )
                            else:
                                outbox.append(
                                    self._wave_msg(
                                        self.waves[w_id]["parent"],
                                        "ECHO",
                                        w_id,
                                        self.clock,
                                        message.action_id,
                                    )
                                )

        return NodeResponse(outbox)

    def _msg(self, to_id, m_type, clock, a_id):
        return Action(
            data={"type": m_type, "sender_id": self.node_id, "clock": clock},
            node_id=to_id,
            action_id=a_id or uuid.uuid4(),
        )

    def _wave_msg(self, to_id, m_type, w_id, clock, a_id):
        return Action(
            data={
                "type": m_type,
                "sender_id": self.node_id,
                "w_id": w_id,
                "clock": clock,
            },
            node_id=to_id,
            action_id=a_id or uuid.uuid4(),
        )
