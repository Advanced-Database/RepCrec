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
            self.write(args[0], args[1], args[2])
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

    def execute_operation_queue(self):
        pass

    # -----------------------------------------------------
    # -------------- Operation Executions ---------------
    # -----------------------------------------------------
    def begin(self, transaction_id):
        if self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "Transaction {} already exists".format(transaction_id))
        self.transaction_table[transaction_id] = Transaction(
            self.ts, transaction_id, False)
        print(transaction_id + " begins")

    def beginro(self, transaction_id):
        if self.transaction_table.get(transaction_id):
            raise InvalidInstructionError("Transaction already exists")
        self.transaction_table[transaction_id] = Transaction(
            self.ts, transaction_id, True)
        print(transaction_id + " begins and is read-only")
        '''
        TO-DO: (Multiversion)
        *** This should be implemented in DataManager. ***
        1. A read-only transaction obtains no locks
        2. It reads all data items that have committed at the time the read transaction begins
        3. As concurrent updates take place, save old copies
        '''

    def add_read_operation(self, transaction_id, variable):
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "Transaction {} does not exist".format(transaction_id))

    def read(self, transaction_id, variable):
        print(transaction_id + " read " + variable)

        for dm in self.data_manager_nodes:
            # If T cannot read a copy of x from a site, then read x from another site
            if dm.is_up and variable in dm.data:
                new_lock = [{
                    "t_id": transaction_id,
                    "lock_type": "r"
                }]
                dm.lock_table[variable] = dm.lock_table.get(
                    variable, []) + new_lock

                # Check if any transaction already held x_lock on the same variable
                has_x_lock = False
                for lock_item in dm.lock_table[variable]:
                    if lock_item["lock_type"] == 'x':
                        has_x_lock, t_with_x_lock = True, lock_item["t_id"]
                        break

                if has_x_lock:
                    # While any T holds an ex_lock on x, no other T may acquire re_lock on x
                    print(
                        "Conflict! " + t_with_x_lock + " is holding the ex_lock for " + variable)
                else:
                    print(variable + ": " + str(dm.data[variable]))
                break

    def write(self, transaction_id, variable, value):
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError(
                "Transaction {} does not exist".format(transaction_id))
        print(transaction_id + " write " +
              variable + " with value '" + value + "'")
        '''
        TO-DO:
        4. Two phases rules: Acquire locks as you go, release locks at end. Implies acquire all locks before releasing any. Based on exclusive locks
        5. If a write from T1 can get some locks but not all, then it is an implementation option whether T1 should release the locks it has or not. However, for purposes of clarity we will say that T1 should release those locks.
        '''

        has_conflict = False
        # If T cannot write a copy of x, then write to all other copies, provided there is at least one
        for dm in self.data_manager_nodes:
            if dm.is_up and variable in dm.data:
                new_lock = [{
                    "t_id": transaction_id,
                    "lock_type": "x",
                    "val": value
                }]
                dm.lock_table[variable] = dm.lock_table.get(
                    variable, []) + new_lock

                # print(dm.lock_table[variable])
                if len(dm.lock_table[variable]) > 1:
                    # While a T holds an read_lock or ex_lock on x, no other T may acquire ex_lock on x
                    has_conflict = True

        if has_conflict:
            print(
                "Conflict! There's at least one transction is holding the lock for " + variable + "in some sites")
        else:
            for dm in self.data_manager_nodes:
                # Available copies allows writes and commits to just the available sites.
                if dm.is_up and variable in dm.data:
                    dm.data[variable] = value
                    print(transaction_id + " write value '" +
                          str(value) + "' into site " + str(dm.site_id))

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
        print("Site " + site_id + " fails")

    def recover(self, site_id):
        print("Site " + site_id + " recovers")
        '''
        TO-DO:
        1. Commit transactions that should be committed and abort the others.
        2. All non-replicated items are available to read and write.
        3. For replicated parts, It allows all transactions to end, but dont start new ones and have this site read copies of its items from any other up site. When done, this service has recovered
        4. The site is up immediately. Allow writes to copies. Reject reads to x until a write to x has occurred.
        5. Abort on Failure
        6. When T1 believes a site is down, all sites must agree. This implies no network partitions
        '''

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
