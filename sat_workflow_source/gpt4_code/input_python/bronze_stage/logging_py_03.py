
ct = datetime.now(pytz.timezone('Asia/Kolkata'))
nt = ct.strftime("%Y%m%d_%H%M%S")

applogger = logging.getLogger(name = "sigraphlogger")
applogger.setLevel(logging.INFO)

Streamhandler = logging.StreamHandler(sys.stdout)

formatter = logging.Formatter(
     fmt      = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
    ,datefmt  = "%m/%d/%Y %I:%M:%S %p"
    ,style    = "%"
    ,validate = True
   )
Streamhandler.setFormatter(formatter)

applogger.addHandler(Streamhandler)