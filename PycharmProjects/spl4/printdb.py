from persistence import *

def main():
    repo.print_tables()
    print()
    repo.print_employees_report()
    print()
    repo.print_activities_report()

if __name__ == '__main__':
    main()