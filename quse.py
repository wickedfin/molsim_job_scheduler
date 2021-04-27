import sys
import pathlib
import collections
import molsim_job_scheduler as mjs

if len(sys.argv) == 2:
    target_user_pattern = sys.argv[1]
else:
    target_user_pattern = ""

config = {}
with open(str(pathlib.Path(__file__).parent / "config.txt"), "r") as f:
    for line in f:
        if line.startswith("#"):
            continue
        name, max_cores, limits = line.split()
        max_cores = int(max_cores)
        config[name] = max_cores

stat_data = mjs.read_stat()
usage = mjs.calculate_running_cores(stat_data)

for user, data in usage.items():
    if not (target_user_pattern in user):
        continue

    print("="*45)
    print("User:", user)
    for k, v in config.items():
        print("{:15s} {:>6d} / {:<6d} ({:>6.1f} %)"
            .format(k, data[k], v, data[k]/v*100)
        )
    print("")

all_users = collections.defaultdict(int)
for user, data in usage.items():
    for k, v in config.items():
        all_users[k] += data[k]

print("="*45)
print("All users")
for k, v in config.items():
    print("{:15s} {:>6d} / {:<6d} ({:>6.1f} %)"
        .format(k, all_users[k], v, all_users[k]/v*100)
    )
