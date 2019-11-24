FILE_PATH = 'test/test1.txt'

if __name__ == '__main__':
    for line in open(FILE_PATH):
        li = line.strip()
        if not li.startswith("//"):
            print(line.rstrip())
