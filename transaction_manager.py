import re
from data_manager import DataManager
from parser import Parser


class InvalidInstructionError(Exception):
    pass


class TransactionManager:
    _parser = Parser()

    def __init__(self):
        # print("Init Transaction Manager!")
        self.data_manager_nodes = []
        for site_id in range(1, 11):
            self.data_manager_nodes.append(DataManager(site_id))

    def process_line(self, line):
        li = self._parser.parse_line(line)
        if li:
            command = li.pop(0)
            try:
                self.execute_instruction(command, li)
            except InvalidInstructionError:
                print("[ERROR] Invalid instruction: " + line.strip())
                return False
        return True

    def execute_instruction(self, command, args):
        if command == "begin":
            self.begin(args[0])
        elif command == "beginRO":
            self.beginro(args[0])
        elif command == "R":
            self.read(args[0], args[1])
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
            raise InvalidInstructionError()

    # -----------------------------------------------------
    # -------------- Instruction Executions ---------------
    # -----------------------------------------------------
    def begin(self, transaction_id):
        print(transaction_id + " begins.")

    def beginro(self, transaction_id):
        print(transaction_id + " begins and is read-only.")
        # TO-DO: (Multiversion)
        # 1. A read-only transaction obtains no locks
        # 2. It reads all data items that have committed at the time the read transaction begins
        # 3. As concurrent updates take place, save old copies

    def read(self, transaction_id, variable):
        print(transaction_id + " read " + variable + ".")
        # TO-DO:
        # 1. If T cannot read a copy of x from a site, then read x from another site.
        # 2. If no relevant site is available, then T must wait.
        # 3. Each variable locks are acquired in a first-come first-serve fashion
        # 4. T acquires a read lock on item x if it only wants to read x.
        # 5. T acquires an exclusive lock on x if it may want to write x.
        # 6. While T holds a read lock on x, no other transaction may acquire an exclusive lock on x.
        # 7. While T holds an exclusive lock on x, no other transaction may acquire any lock on x.

    def write(self, transaction_id, variable, value):
        print(transaction_id + " write " +
              variable + " with value " + value + ".")
        # TO-DO:
        # 1. If T cannot write a copy of x, then write to all other copies, provided there is at least one.
        # 2. Available copies allows writes and commits to just the available sites.
        # 3. Each variable locks are acquired in a first-come first-serve fashion
        # 4. Two phases rules: Acquire locks as you go, release locks at end. Implies acquire all locks before releasing any. Based on exclusive locks
        # 5. If a write from T1 can get some locks but not all, then it is an implementation option whether T1 should release the locks it has or not. However, for purposes of clarity we will say that T1 should release those locks.

    def dump(self):
        print("Dump all data at all sites!")
        for dm in self.data_manager_nodes:
            if dm.is_up:
                print("Site" + str(dm.id) + " is up")
            else:
                print("Site" + str(dm.id) + "'s status: Down")

            dm_info = dm.dump(dm.id)
            print("Site" + str(dm.id) + "'s data: " + dm_info)

    def end(self, transaction_id):
        print(transaction_id + " ends (commits or aborts).")
        # TO-DO:
        # 1. At Commit time, for two phase locked transactions: ensure that all servers that you accessed (read or write) have been up since the first time they were accessed. Otherwise, abort. (Read-only transactions need not abort in this case.)
        # 2. end(T1) causes your system to report whether T1 can commit in the format T1 commits or T1 aborts
        # 3. If a transaction accesses an item (really accesses it, not just request a lock) at a site and the site then fails, then transaction should continue to execute and then abort only at its commit time.

    def fail(self, site_id):
        print("Site " + site_id + " fails.")

    def recover(self, site_id):
        print("Site " + site_id + " recovers.")
        # TO-DO:
        # 1. Commit transactions that should be committed and abort the others.
        # 2. All non-replicated items are available to read and write.
        # 3. For replicated parts, It allows all transactions to end, but don’t start new ones and have this site read copies of its items from any other up site. When done, this service has recovered
        # 4. The site is up immediately. Allow writes to copies. Reject reads to x until a write to x has occurred.
        # 5. Abort on Failure
        # 6. When T1 believes a site is down, all sites must agree. This implies no network partitions

    # -----------------------------------------------------
    # -----------------------------------------------------
    # -----------------------------------------------------

    def deadlock_detection(self):
        print("Execute deadlock detection!")
        # TO-DO:
        # 1. Construct a blocking (waits-for) graph. Give each transaction a unique timestamp. Require that numbers given to transactions always increase.
        # 2. Detect deadlocks using cycle detection and abort the youngest transaction in the cycle.
        # 3. Cycle detection need not happen frequently. Deadlock doesn’t go away.

    def monitor_site_status(self):
        for dm in self.data_manager_nodes:
            if dm.is_up:
                pass
            else:
                pass
