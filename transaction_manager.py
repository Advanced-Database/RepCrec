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
    def __init__(self, command, transaction_id, variable, value=None):
        self.command = command
        self.transaction_id = transaction_id
        self.variable = variable
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

    def add_read_operation(self, transaction_id, variable):
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "{} does not exist".format(transaction_id))
        self.operation_queue.append(
            Operation("R", transaction_id, variable))

    def add_write_operation(self, transaction_id, variable, value):
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "{} does not exist".format(transaction_id))
        self.operation_queue.append(
            Operation("W", transaction_id, variable, value))

    def execute_operation_queue(self):
        for op in list(self.operation_queue):
            success = False
            if op.command == "R":
                success = self.read(op.transaction_id, op.variable)
            elif op.command == "W":
                success = self.write(op.transaction_id, op.variable, op.value)
            else:
                print("Invalid operation!")
            if success:
                self.operation_queue.remove(op)

    # -----------------------------------------------------
    # -------------- Operation Executions ---------------
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
        '''
        TO-DO: (Multiversion)
        *** This should be implemented in DataManager. ***
        1. A read-only transaction obtains no locks
        2. It reads all data items that have committed at the time the read transaction begins
        3. As concurrent updates take place, save old copies
        '''

    def read(self, transaction_id, variable):
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "Transaction {} does not exist".format(transaction_id))

        for dm in self.data_manager_nodes:
            if dm.get_read_lock(variable):
                return dm.set_read_lock(variable)
        return False

    def write(self, transaction_id, variable, value):
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "Transaction {} does not exist".format(transaction_id))

        print(transaction_id + " write " +
              variable + " with value '" + value + "'")
        for dm in self.data_manager_nodes:
            if dm.is_exclusive_lock_conflict(variable):
                return False

        for dm in self.data_manager_nodes:
            if dm.get_exclusive_lock(variable):
                dm.set_exclusive_lock(variable)
        return True
        '''
        TO-DO:
        4. Two phases rules: Acquire locks as you go, release locks at end. Implies acquire all locks before releasing any. Based on exclusive locks
        5. If a write from T1 can get some locks but not all, then it is an implementation option whether T1 should release the locks it has or not. However, for purposes of clarity we will say that T1 should release those locks.
        '''

    def dump(self):
        print("Dump all data at all sites!")
        for dm in self.data_manager_nodes:
            if dm.is_up:
                print("Site" + str(dm.site_id) + " is up")
            else:
                print("Site" + str(dm.site_id) + "'s status: Down")

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
        #     for variable in dm.lock_table.keys():
        #         lock_item = dm.lock_table[variable]
        #         if lock_item[0] == transaction_id and lock_item[1] == 'x':
        #             dm.data[variable] = lock_item[2]
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
