import re
from data_manager import DataManager
from parser import Parser


class InvalidInstructionError(Exception):
    pass


class TransactionManager:
    _parser = Parser()
    re_locks = {}
    ex_locks = {}

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
        '''
        TO-DO: (Multiversion)
        1. A read-only transaction obtains no locks
        2. It reads all data items that have committed at the time the read transaction begins
        3. As concurrent updates take place, save old copies
        '''

    def read(self, transaction_id, variable):
        print(transaction_id + " read " + variable + ".")

        if variable in self.ex_locks:
            # While a T holds an ex_lock on x, no other T may acquire read_lock on x
            print("Conflict! " + self.ex_locks[variable] +
                  " is holding the ex_lock for " + variable)
        else:
            # T acquires a re_lock on item x if it only wants to read x
            if variable not in self.re_locks:
                self.re_locks[variable] = [transaction_id]
            else:
                self.re_locks[variable].append(transaction_id)
            print("Success! " + transaction_id +
                  " gets a re_locks for " + variable)

        # If T cannot read a copy of x from a site, then read x from another site
        isRead, var_val = False, None
        for dm in self.data_manager_nodes:
            if dm.is_up and variable in dm.data:
                var_val = dm.data[variable]
                isRead = True
                print(transaction_id + " reads a copy of " + variable +
                      " with value '" + str(var_val) + "' from site_" + str(dm.site_id))
                break
            else:
                print(transaction_id + " cannot read a copy of " + variable +
                      " from site_" + str(dm.site_id))
        # If no relevant site is available, then T must wait
        if not isRead:
            print("No relevant site is available, then " +
                  transaction_id + " must wait")

    def write(self, transaction_id, variable, value):
        print(transaction_id + " write " +
              variable + " with value " + value + ".")
        '''
        TO-DO:
        1. If T cannot write a copy of x, then write to all other copies, provided there is at least one.
        2. Available copies allows writes and commits to just the available sites.
        4. Two phases rules: Acquire locks as you go, release locks at end. Implies acquire all locks before releasing any. Based on exclusive locks
        5. If a write from T1 can get some locks but not all, then it is an implementation option whether T1 should release the locks it has or not. However, for purposes of clarity we will say that T1 should release those locks.
        '''

        # While a T holds an read_lock or ex_lock on x, no other T may acquire ex_lock on x
        if variable in self.re_locks or variable in self.ex_locks:
            if variable in self.re_locks:
                print("Conflict! " + str(self.re_locks[variable]) +
                      " is holding a re_lock for " + variable)
            if variable in self.ex_locks:
                print("Conflict! " + self.ex_locks[variable] +
                      " is holding a ex_lock for " + variable)
        else:
            # T acquires an ex_lock on x if it may want to write x
            self.ex_locks[variable] = transaction_id
            print("Success! " + transaction_id +
                  " gets a ex_lock for " + variable)

            # If T cannot write a copy of x, then write to all other copies, provided there is at least one
            isWrite = False
            for dm in self.data_manager_nodes:
                if dm.is_up and variable in dm.data:
                    dm.data[variable] = value
                    isWrite = True

            if isWrite:
                print("Success! " + transaction_id +
                      " write " + value + " into all the available copies of " + variable)

    def dump(self):
        print("Dump all data at all sites!")
        for dm in self.data_manager_nodes:
            if dm.is_up:
                print("Site" + str(dm.site_id) + " is up")
            else:
                print("Site" + str(dm.site_id) + "'s status: Down")

            dm_info = dm.dump(dm.site_id)
            print("Site" + str(dm.site_id) + "'s data: " + dm_info)

    def end(self, transaction_id):
        print(transaction_id + " ends (commits or aborts).")
        '''
        TO-DO:
        1. At Commit time, for two phase locked transactions: ensure that all servers that you accessed (read or write) have been up since the first time they were accessed. Otherwise, abort. (Read-only transactions need not abort in this case.)
        2. end(T1) causes your system to report whether T1 can commit in the format T1 commits or T1 aborts
        3. If a transaction accesses an item (really accesses it, not just request a lock) at a site and the site then fails, then transaction should continue to execute and then abort only at its commit time.
        '''

    def fail(self, site_id):
        print("Site " + site_id + " fails.")

    def recover(self, site_id):
        print("Site " + site_id + " recovers.")
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
