import argparse
import os
import time


def happy_path(sleep_secs, output_file_path, task_name):
    time.sleep(sleep_secs)
    f = open(output_file_path, "w")
    f.write("Mock output from '{}'\n".format(task_name))
    f.close()


def main():
    """
    The remote script that MockSleepAndWrite calls
    """

    parser = argparse.ArgumentParser(description="mock job")
    parser.add_argument(
        "--sleep_secs",
        default=10,
        type=int,
        action="store",
        help="The number of seconds to sleep before writing " "or dying",
    )
    parser.add_argument(
        "--output_file_path",
        default="/tmp/jobmon-test",
        type=str,
        action="store",
        help="Full path to output file",
    )
    parser.add_argument(
        "--task_name",
        default="nameless",
        type=str,
        action="store",
        help="Task name, written to output file for debugging",
    )
    parser.add_argument(
        "--fail_always",
        action="store_true",
        default=False,
        help="If true, sleep and then raise a ValueError",
    )
    parser.add_argument(
        "--fail_count",
        type=int,
        default=0,
        action="store",
        help="If true, then check the a file for the count of "
        "previous failures (stateful!)."
        "If previous fails < fail_count then fail",
    )
    args = parser.parse_args()

    if args.fail_always:
        time.sleep(args.sleep_secs)
        raise ValueError("Mock task failing permanently by command line arg")
    elif args.fail_count:
        # Go check how many times this script has failed
        counter_file = "{}-count".format(args.output_file_path)
        if os.path.exists(counter_file):
            # Not the first time, let's see how many times we have failed
            fp = open(counter_file, "r")
            count_so_far = int(fp.read())
            fp.close()
            os.remove(counter_file)
        else:
            # First time, have not yet failed
            count_so_far = 0

        if count_so_far < args.fail_count:
            # Have not yet failed enuf
            count_so_far += 1
            fp = open(counter_file, "w")
            fp.write("{}\n".format(count_so_far))
            fp.close()
            raise ValueError("Mock task failing intermittently {}".format(count_so_far))
        else:
            # Enough failures, we should succeed this time
            happy_path(args.sleep_secs, args.output_file_path, args.task_name)
    else:
        # No "fail" argument, we should succeed
        happy_path(args.sleep_secs, args.output_file_path, args.task_name)


if __name__ == "__main__":
    main()
