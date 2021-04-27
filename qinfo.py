import molsim_job_scheduler as mjs

jobs = mjs.read_jobs()
print("="*(15+20+25+15))
print("{:15s} {:20s} {:25s} {:15s}".format("Id", "Nodes", "File", "User"))
print("="*(15+20+25+15))
for job in jobs:
    print("{:<15d} {:20s} {:<25s} {:<15s}"
        .format(job.id, job.nodes, job.file[:25], job.user)
    )
