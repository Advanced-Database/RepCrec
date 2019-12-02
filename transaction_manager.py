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

    def read(self, transaction_id, variable):
        # transaction_id = instr_line[1]
        # variable = instr_line[2]
        print(transaction_id + " read " + variable + ".")

    def write(self, transaction_id, variable, value):
        # transaction_id = instr_line[1]
        # variable = instr_line[2]
        # value = instr_line[3]
        print(transaction_id + " write " + variable +
              " with value " + value + ".")

    def dump(self):
        print("Dump all data at all sites!")

    def end(self, transaction_id):
        print(transaction_id + " ends (commits or aborts).")

    def fail(self, site_id):
        print("Site " + site_id + " fails.")

    def recover(self, site_id):
        print("Site " + site_id + " recovers.")

    # -----------------------------------------------------
    # -----------------------------------------------------
    # -----------------------------------------------------

    def monitor_site_status(self):
        for dm in self.data_manager_nodes:
            if dm.is_up:
                pass
            else:
                pass

    def output_site_status(self):
        print("-------------------- Dump all the output --------------------")
        for dm in self.data_manager_nodes:
            if dm.is_up:
                print("Site" + str(dm.id) + " is up")
            else:
                print("Site" + str(dm.id) + "'s status: Down")

            dm_info = dm.dump(dm.id)
            print("Site" + str(dm.id) + "'s data: " + dm_info)
