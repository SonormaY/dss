import uuid
from typing import List
from scheduler.implementation.node import Node
from scheduler.core.action import Action
from scheduler.core.node_response import NodeResponse


class MerlinSegallNode(Node):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        super().__init__(node_id, neighbors)
        self.my_dist = float("inf")
        self.routing_parent = None
        self.current_phase = 0
        self.pif_parent = None
        self.received_infos = set()
        self.children = set()
        self.received_echos = set()
        self.neighbor_dists = {}
        self.changed_in_phase = False
        self.is_root = False
        self.echo_sent = False
        self.computed_in_phase = False

    def start_phase(self, phase: int, action_id: uuid.UUID) -> List[Action]:
        self.current_phase = phase
        self.received_infos.clear()
        self.children.clear()
        self.received_echos.clear()
        self.neighbor_dists.clear()
        self.changed_in_phase = False
        self.echo_sent = False
        self.computed_in_phase = False

        if self.is_root:
            self.pif_parent = self.node_id

        outbox = []
        for neighbor in self.neighbors:
            outbox.append(
                Action(
                    data={
                        "type": "INFO",
                        "sender_id": self.node_id,
                        "phase": self.current_phase,
                        "dist": self.my_dist,
                        "pif_parent": self.pif_parent,
                    },
                    node_id=neighbor,
                    action_id=action_id or uuid.uuid4(),
                )
            )
        return outbox

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data
        msg_type = data.get("type", data.get("message_type"))
        sender_id = data.get("sender_id")
        outbox = []

        if msg_type == "New":
            if self.current_phase == 0:
                self.is_root = True
                self.my_dist = 0
                self.routing_parent = self.node_id
                print(
                    f"\n[INIT] Node {self.node_id} is the SINK (ROOT). Starting Merlin-Segall Phase 1.\n"
                )
                outbox.extend(self.start_phase(1, message.action_id))

        elif msg_type == "INFO":
            phase = data.get("phase")
            dist = data.get("dist")
            parent_id = data.get("pif_parent")

            if phase > self.current_phase:
                self.pif_parent = sender_id
                outbox.extend(self.start_phase(phase, message.action_id))

            if phase == self.current_phase:
                self.received_infos.add(sender_id)
                self.neighbor_dists[sender_id] = dist
                if parent_id == self.node_id:
                    self.children.add(sender_id)

                self._check_completion(message.action_id, outbox)

        elif msg_type == "ECHO":
            phase = data.get("phase")
            changed = data.get("changed")

            if phase == self.current_phase:
                self.received_echos.add(sender_id)
                self.changed_in_phase = self.changed_in_phase or changed
                self._check_completion(message.action_id, outbox)

        return NodeResponse(outbox)

    def _check_completion(self, action_id: uuid.UUID, outbox: List[Action]):
        if len(self.received_infos) == len(self.neighbors):
            if not self.is_root and not self.computed_in_phase:
                self.computed_in_phase = True
                min_neighbor_dist = min(self.neighbor_dists.values())

                if min_neighbor_dist == float("inf"):
                    new_dist = float("inf")
                    new_routing_parent = None
                else:
                    new_dist = min_neighbor_dist + 1
                    best_parents = [
                        n
                        for n, d in self.neighbor_dists.items()
                        if d == min_neighbor_dist
                    ]
                    best_parents.sort(key=str)
                    new_routing_parent = best_parents[0]

                changed = (new_dist != self.my_dist) or (
                    new_routing_parent != self.routing_parent
                )

                if changed:
                    self.my_dist = new_dist
                    self.routing_parent = new_routing_parent
                    self.changed_in_phase = True
                    print(
                        f"[COMPUTE] Node {self.node_id} (Phase {self.current_phase}): dist={self.my_dist}, parent={str(self.routing_parent)[:8]}"
                    )

            if len(self.received_echos) == len(self.children):
                if not self.echo_sent:
                    self.echo_sent = True
                    if self.is_root:
                        print(
                            f"\n[PHASE END] Root completed phase {self.current_phase}. Changes occurred: {self.changed_in_phase}\n"
                        )
                        if self.changed_in_phase:
                            outbox.extend(
                                self.start_phase(self.current_phase + 1, action_id)
                            )
                        else:
                            print(
                                "\n[TERMINATION] >>> MERLIN-SEGALL ALGORITHM FINISHED! Shortest paths sink tree is built. <<<\n"
                            )
                    else:
                        outbox.append(
                            Action(
                                data={
                                    "type": "ECHO",
                                    "sender_id": self.node_id,
                                    "phase": self.current_phase,
                                    "changed": self.changed_in_phase,
                                },
                                node_id=self.pif_parent,
                                action_id=action_id or uuid.uuid4(),
                            )
                        )
