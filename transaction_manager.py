import re
from data_manager import DataManager
from parser import Parser


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


class TransactionManager:
    parser = Parser()
    transaction_table = {}
    ts = 0
    operation_queue = []

    def __init__(self):
        # print("Init Transaction Manager!")
        self.data_manager_nodes = []
        for site_id in range(1, 11):
            self.data_manager_nodes.append(DataManager(site_id))

    def process_line(self, line):
        li = self.parser.parse_line(line)
        if li:
            command = li.pop(0)
            try:
                print("----- Timestamp: " + str(self.ts) + " -----")
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
            self.end(args[0])
        elif command == "fail":
            self.fail(args[0])
        elif command == "recover":
            self.recover(args[0])
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
                self.operation_queue.remove(op)

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
        for dm in self.data_manager_nodes:
            if dm.is_up:
                result = dm.read_snapshot(variable_id, ts)
                if result.success:
                    print("{} read_ro {}.{}: {}".format(
                        transaction_id, variable_id, dm.site_id, result.value))
                    return True
        return False

    def read(self, transaction_id, variable_id):
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "Transaction {} does not exist".format(transaction_id))

        for dm in self.data_manager_nodes:
            if dm.is_up and dm.has_variable(variable_id):
                result = dm.read(transaction_id, variable_id)
                if result.success:
                    self.transaction_table[
                        transaction_id].sites_accessed.append(dm.site_id)
                    print("{} read {}.{}: {}".format(
                        transaction_id, variable_id, dm.site_id, result.value))
                    return True
        return False

    def write(self, transaction_id, variable_id, value):
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "Transaction {} does not exist".format(transaction_id))
        all_relevant_sites_down = True
        can_get_all_write_locks = True
        for dm in self.data_manager_nodes:
            if dm.is_up and dm.has_variable(variable_id):
                all_relevant_sites_down = False
                result = dm.get_write_lock(transaction_id, variable_id)
                if not result:
                    can_get_all_write_locks = False
        if not all_relevant_sites_down and can_get_all_write_locks:
            for dm in self.data_manager_nodes:
                if dm.is_up and dm.has_variable(variable_id):
                    dm.write(transaction_id, variable_id, value)
                    self.transaction_table[
                        transaction_id].sites_accessed.append(dm.site_id)
                    print("{} write {}.{} with value {}".format(
                        transaction_id, variable_id, dm.site_id, value))
            return True
        return False

    def dump(self):
        print("Dump all data at all sites!")
        for dm in self.data_manager_nodes:
            if dm.is_up:
                print("Site " + str(dm.site_id) + " is up")
            else:
                print("Site " + str(dm.site_id) + "'s status: Down")

            print(dm.dump(dm.site_id))

    def end(self, transaction_id):
        print(transaction_id + " ends (commits or aborts).")
        '''
        TO-DO:
        1. At Commit time, for two phase locked transactions: ensure that all servers that you accessed (read or write) have been up since the first time they were accessed. Otherwise, abort. (Read-only transactions need not abort in this case.)
        2. end(T1) causes your system to report whether T1 can commit in the format T1 commits or T1 aborts
        3. If a transaction accesses an item (really accesses it, not just request a lock) at a site and the site then fails, then transaction should continue to execute and then abort only at its commit time.
        '''
        # for dm in self.data_manager_nodes:
        #     for variable_id in dm.lock_table.keys():
        #         lock_item = dm.lock_table[variable_id]
        #         if lock_item[0] == transaction_id and lock_item[1] == 'x':
        #             dm.data[variable_id] = lock_item[2]
        # if dm.is_up:
        # else:

    def fail(self, site_id):
        site = self.data_manager_nodes[int(site_id) - 1]
        if not site.is_up:
            raise InvalidInstructionError(
                "Site {} is already down".format(site_id))
        site.fail(self.ts)
        for t in self.transaction_table.values():
            if (not t.is_ro) and (not t.will_abort) and (
                    site_id in t.sites_accessed):
                t.will_abort = True
        print("Site " + site_id + " fails")

    def recover(self, site_id):
        site = self.data_manager_nodes[int(site_id) - 1]
        if site.is_up:
            raise InvalidInstructionError(
                "Site {} is already up".format(site_id))
        site.recover(self.ts)
        print("Site " + site_id + " recovers")

    # -----------------------------------------------------
    # -----------------------------------------------------
    # -----------------------------------------------------

    def deadlock_detection(self):
        print("Execute deadlock detection!")
        '''
        TO-DO:
        1. Construct a blocking (waits-for) graph. Give each transaction a unique timestamp. Require that numbers given to transactions always increase.
        2. Detect deadlocks using cycle detection and abort the youngest transaction in the cycle.
        3. Cycle detection need not happen frequently. Deadlock doesnt go away.
        '''

    def monitor_site_status(self):
        for dm in self.data_manager_nodes:
            if dm.is_up:
                pass
            else:
                pass
