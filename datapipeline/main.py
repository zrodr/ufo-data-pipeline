from sys import argv
from application import Application

if __name__ == "__main__":
    app = Application(argv[1:])
    app.run()