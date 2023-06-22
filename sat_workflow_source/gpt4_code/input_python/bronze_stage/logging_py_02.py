
# ct = datetime.now(pytz.timezone('Asia/Kolkata'))
# nt = ct.strftime("%Y%m%d_%H%M%S")
# print(f"Current timestamp : {nt}")

# partition_path = ct.strftime("%Y/%m/%d/")
# print(f"partition path : {partition_path}")

# directory = "/tmp/"
# logfilename = "sigraph_"+nt+".log"
# finalpath = directory+logfilename
# print(f"finalpath : {finalpath}")

# applogger = logging.getLogger(name = "sigraphlogger")
# applogger.setLevel(logging.INFO)

# Filehandler = logging.FileHandler(finalpath, mode = "a")

# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s', datefmt="%m/%d/%Y %I:%M:%S %p")
# Filehandler.setFormatter(formatter)

# applogger.addHandler(Filehandler)

