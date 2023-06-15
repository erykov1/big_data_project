from scheduler.trigger import Monday
import commands
from scheduler import Scheduler
import datetime as datetime


def main():
    sch = Scheduler().weekly(datetime.time(21, 34), commands.update_for_scheduler(), datetime.timedelta(weeks=1))


if __name__ == "__main__":
    main()