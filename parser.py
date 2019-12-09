import re


class Parser:
    debug_info_below = False

    def parse_line(self, line):
        """
        Parse one line of input into command and arguments.
        :param line: one line of input
        :return: a list of strings consisting of command and arguments
        """
        if self.debug_info_below:
            return None
        li = line.split('//')[0].strip()
        if li:
            if li.startswith("==="):
                self.debug_info_below = True
                return None
            return re.findall(r"[\w']+", li)
