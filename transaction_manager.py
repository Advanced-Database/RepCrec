from data_manager import DataManager
from parser import Parser
from collections import defaultdict


class InvalidInstructionError(Exception):
    def __init__(self, message):
        self.message = message


class Transaction:
    def __init__(self, ts, transaction_id, is_ro):
        self.ts = ts
        self.transaction_id = transaction_id
        self.is_ro = is_ro
        self.will_abort = False
        self.sites_accessed = []


class Operation:
    def __init__(self, command, transaction_id, variable_id, value=None):
        self.command = command
        self.transaction_id = transaction_id
        self.variable_id = variable_id
        self.value = value

    def __repr__(self):
        if self.value is None:
            return "{}({},{})".format(self.command, self.transaction_id,
                                      self.variable_id)
        return "{}({},{},{})".format(self.command, self.transaction_id,
                                     self.variable_id, self.value)


class TransactionManager:
    parser = Parser()
    transaction_table = {}
    ts = 0
    operation_queue = []

    def __init__(self):
        self.data_manager_list = []
        for site_id in range(1, 11):
            self.data_manager_list.append(DataManager(site_id))

    def process_line(self, line):
        li = self.parser.parse_line(line)
        if li:
            command = li.pop(0)
            try:
                print("----- Timestamp: " + str(self.ts) + " -----")
                if self.deadlock_detection():
                    self.execute_operation_queue()
                self.process_instruction(command, li)
                self.execute_operation_queue()
                self.ts += 1
            except InvalidInstructionError as e:
                print("[INVALID_INSTRUCTION_ERROR] " + e.message +
                      ": " + line.strip())
                return False
        return True

    def process_instruction(self, command, args):
        if command == "begin":
            self.begin(args[0])
        elif command == "beginRO":
            self.beginro(args[0])
        elif command == "R":
            self.add_read_operation(args[0], args[1])
        elif command == "W":
            self.add_write_operation(args[0], args[1], args[2])
        elif command == "dump":
            self.dump()
        elif command == "end":
            self.end(args[0], self.ts)
        elif command == "fail":
            self.fail(int(args[0]))
        elif command == "recover":
            self.recover(int(args[0]))
        else:
            raise InvalidInstructionError("Unknown instruction")

    def add_read_operation(self, transaction_id, variable_id):
        self.operation_queue.append(
            Operation("R", transaction_id, variable_id))

    def add_write_operation(self, transaction_id, variable_id, value):
        self.operation_queue.append(
            Operation("W", transaction_id, variable_id, value))

    def execute_operation_queue(self):
        for op in list(self.operation_queue):
            if not self.transaction_table.get(op.transaction_id):
                self.operation_queue.remove(op)
            else:
                success = False
                if op.command == "R":
                    if self.transaction_table[op.transaction_id].is_ro:
                        success = self.read_snapshot(op.transaction_id,
                                                     op.variable_id)
                    else:
                        success = self.read(op.transaction_id, op.variable_id)
                elif op.command == "W":
                    success = self.write(op.transaction_id, op.variable_id,
                                         op.value)
                else:
                    print("Invalid operation!")
                if success:
                    # print("Executed op: {}".format(op))
                    self.operation_queue.remove(op)
        # print("Remaining ops: {}".format(self.operation_queue))

    # -----------------------------------------------------
    # -------------- Instruction Executions ---------------
    # -----------------------------------------------------
    def begin(self, transaction_id):
        if self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "{} already exists".format(transaction_id))
        self.transaction_table[transaction_id] = Transaction(
            self.ts, transaction_id, False)
        print("{} begins".format(transaction_id))

    def beginro(self, transaction_id):
        if self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "{} already exists".format(transaction_id))
        self.transaction_table[transaction_id] = Transaction(
            self.ts, transaction_id, True)
        print("{} begins and is read-only".format(transaction_id))

    def read_snapshot(self, transaction_id, variable_id):
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "Transaction {} does not exist".format(transaction_id))
        ts = self.transaction_table[transaction_id].ts
        for dm in self.data_manager_list:
            if dm.is_up and dm.has_variable(variable_id):
                result = dm.read_snapshot(variable_id, ts)
                if result.success:
                    print("{} (RO) reads {}.{}: {}".format(
                        transaction_id, variable_id, dm.site_id, result.value))
                    return True
        return False

    def read(self, transaction_id, variable_id):
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "Transaction {} does not exist".format(transaction_id))

        for dm in self.data_manager_list:
            if dm.is_up and dm.has_variable(variable_id):
                result = dm.read(transaction_id, variable_id)
                if result.success:
                    self.transaction_table[
                        transaction_id].sites_accessed.append(dm.site_id)
                    print("{} reads {}.{}: {}".format(
                        transaction_id, variable_id, dm.site_id, result.value))
                    return True
        return False

    def write(self, transaction_id, variable_id, value):
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "Transaction {} does not exist".format(transaction_id))
        all_relevant_sites_down = True
        can_get_all_write_locks = True
        for dm in self.data_manager_list:
            if dm.is_up and dm.has_variable(variable_id):
                all_relevant_sites_down = False
                result = dm.get_write_lock(transaction_id, variable_id)
                if not result:
                    can_get_all_write_locks = False
        if not all_relevant_sites_down and can_get_all_write_locks:
            # print("{} will write {} with value {}".format(
            #     transaction_id, variable_id, value))
            sites_written = []
            for dm in self.data_manager_list:
                if dm.is_up and dm.has_variable(variable_id):
                    dm.write(transaction_id, variable_id, value)
                    self.transaction_table[
                        transaction_id].sites_accessed.append(dm.site_id)
                    sites_written.append(dm.site_id)
            print("{} writes {} with value {} to sites {}".format(
                transaction_id, variable_id, value, sites_written))
            return True
        return False

    def dump(self):
        print("Dump:")
        for dm in self.data_manager_list:
            dm.dump()

    def end(self, transaction_id, ts):
        if self.transaction_table[transaction_id].will_abort:
            self.abort(transaction_id, True)
        else:
            self.commit(transaction_id, ts)

    def abort(self, transaction_id, due_to_site_fail=False):
        for dm in self.data_manager_list:
            dm.abort(transaction_id)
        self.transaction_table.pop(transaction_id)
        if due_to_site_fail:
            print("{} aborts! (Due to site failure)".format(transaction_id))
        else:
            print("{} aborts! (Due to deadlock)".format(transaction_id))

    def commit(self, transaction_id, commit_ts):
        for dm in self.data_manager_list:
            dm.commit(transaction_id, commit_ts)
        self.transaction_table.pop(transaction_id)
        print("{} commits!".format(transaction_id))

    def fail(self, site_id):
        dm = self.data_manager_list[site_id - 1]
        if not dm.is_up:
            raise InvalidInstructionError(
                "Site {} is already down".format(site_id))
        dm.fail(self.ts)
        print("Site {} fails".format(site_id))
        for t in self.transaction_table.values():
            if (not t.is_ro) and (not t.will_abort) and (
                    site_id in t.sites_accessed):
                t.will_abort = True

    def recover(self, site_id):
        dm = self.data_manager_list[site_id - 1]
        if dm.is_up:
            raise InvalidInstructionError(
                "Site {} is already up".format(site_id))
        dm.recover(self.ts)
        print("Site {} recovers".format(site_id))

    # -----------------------------------------------------
    # -----------------------------------------------------
    # -----------------------------------------------------

    def deadlock_detection(self):
        # print("Executing deadlock detection!")
        blocking_graph = defaultdict(set)
        for dm in self.data_manager_list:
            if dm.is_up:
                graph = dm.generate_blocking_graph()
                for node, adj_list in graph.items():
                    blocking_graph[node].update(adj_list)
        # print(dict(blocking_graph))
        youngest_t_id = None
        youngest_ts = -1
        for node in list(blocking_graph.keys()):
            visited = set()
            if has_cycle(node, node, visited, blocking_graph):
                if self.transaction_table[node].ts > youngest_ts:
                    youngest_t_id = node
                    youngest_ts = self.transaction_table[node].ts
        if youngest_t_id:
            print("Deadlock detected: aborting {}".format(youngest_t_id))
            self.abort(youngest_t_id)
            return True
        return False


def has_cycle(current, root, visited, blocking_graph):
    visited.add(current)
    for neighbour in blocking_graph[current]:
        if neighbour == root:
            return True
        if neighbour not in visited:
            if has_cycle(neighbour, root, visited, blocking_graph):
                return True
    return False
