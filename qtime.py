import molsim_job_scheduler as mjs

stat_data = mjs.read_stat()
usage = mjs.calculate_usage(stat_data)
mjs.print_usage(usage)
