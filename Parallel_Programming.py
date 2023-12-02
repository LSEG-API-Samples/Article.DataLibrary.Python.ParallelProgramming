# Basic Python libraries
import time
from enum import Enum

# The main processing class built to compare the different approaches
from data_processor import DataProcessor

# Enums used for our inputs
class Algorithm(Enum):
    NATIVE = 1              # Native library optimizations
    THREAD = 2              # Using Pythons Threading capabilities
    PROCESS = 3             # Using Pythons Process (launching child processes) capabilities
    HYBRID = 4              # A combination of Threading and Process

# Some global variables representing value setting for out tests
algorithm = Algorithm.NATIVE        # Default algorithm
universe = []                       # Universe of items - subset of instrument list
fields = []                         # Fields specified in request

# Main instruments file containing our test items
INSTRUMENTS_FILE = "Instruments.txt"
INSTRUMENTS_LIST = []
DEFAULT_UNIVERSE_SIZE = 7500

# =========================================================================
# Main Menu 
# Capture input to perform tests.
def main_menu():   
    global algorithm, universe
    
    # Main processing class
    processor = DataProcessor()
    
    batch_size = processor.NB_ITEMS_THRESHOLD
    child_cnt = processor.CHILD_PROCESSES
    
    while processor.is_session_opened():
        print("\nTesting platform to determine optimal processing for your specific hardware\n")
        print("Main Menu:")
        print(f"\t1. Processing algorithm ({algorithm.name})")
        print(f"\t2. Universe size ({len(universe)})")

        if algorithm in {Algorithm.THREAD, Algorithm.HYBRID}:
            print(f"\t3. Bucket size ({batch_size})")
        if algorithm in {Algorithm.PROCESS, Algorithm.HYBRID}:
            print(f"\t4. Child Processes ({child_cnt})")
        print("\n\tE. Execute")
        print("\tX. Exit")
        option = input("\n\tOption ==> ")

        if option.isdigit():
            option = int(option)
            
            try:
                # Handle the option
                if option == 1:
                    algorithm = select_algorithm()
                elif option == 2:
                    num = int(input("\nSize of universe: "))
                    if num > 0:
                        universe = INSTRUMENTS_LIST[:num]
                elif option == 3 and algorithm in {Algorithm.THREAD, Algorithm.HYBRID}:
                    num = int(input("\nBucket size: "))
                    if num > 0:
                        batch_size = num
                elif option == 4 and algorithm in {Algorithm.PROCESS, Algorithm.HYBRID}:
                    num = int(input("\nMax child processes: "))
                    if num > 0:
                        child_cnt = num
                        
            except:
                pass;  # Invalid input

        elif option.lower() == 'e':
            execute(processor, batch_size, child_cnt)
        elif option.lower() == 'x':
            print("Exiting...")
            processor.close()
            return
         
def select_algorithm():
    print(f"\nSelect an algorithm (Currently: {algorithm.name})")
    print("\t1. NATIVE (Use the native library optimizations)")
    print("\t2. THREADS (Use Pythons concurrent capabilities to distribute work on multiple threads)")
    print("\t3. PROCESSES (Use Pythons concurrent capabilities to distribute work across multiple child processes)")
    print("\t4. HYBRID (Use a combination of THREADS and PROCESSES capabilities)")
    option = input("\n\tOption ==> ")
    
    if option.isdigit():
        option = int(option)
        
        # Handle the option
        if option == 1:
            return Algorithm.NATIVE
        elif option == 2:
            return Algorithm.THREAD
        elif option == 3:
            return Algorithm.PROCESS
        elif option == 4:
            return Algorithm.HYBRID
            
    return algorithm
    
    
def init():
    global fields, universe
       
    fields = ["TR.CommonName", "TR.ISINCode", "TR.PriceClose.Currency", "TR.HeadquartersCountry", "TR.TRBCEconomicSector",
              "TR.TRBCBusinessSector", "TR.PriceMainIndexRIC", "TR.FreeFloat", "TR.SharesOutstanding", "TR.CompanyMarketCapitalization",
              "TR.IssuerRating(IssuerRatingSrc=SPI,RatingScope=DMS)", "TR.IssuerRating(IssuerRatingSrc=MIS,RatingScope=DMS)",
              "TR.IssuerRating(IssuerRatingSrc=FDL,RatingScope=DMS)", "TR.PriceClose", "TR.PricePctChg1D", "TR.Volatility5D",
              "TR.Volatility10D", "TR.Volatility30D", "TR.RSISimple14D", "TR.WACCBeta", "TR.BetaFiveYear", "TR.DivAnnouncementDate",
              "TR.DivExDate", "TR.DivPayDate", "TR.DivAdjustedGross", "TR.DivAdjustedNet", "TR.RelValPECOmponent",
              "TR.RelValEVEBITDACOmponent", "TR.RelValDividendYieldCOmponent", "TR.RelValEVSalesCOmponent",
              "TR.RelValPriceCashFlowCOmponent", "TR.RelValPriceBookCOmponent", "TR.RecMean", "TR.RecLabel", "TR.RevenueSmartEst",
              "TR.RevenueMean", "TR.RevenueMedian", "TR.NetprofitSmartEst", "TR.NetProfitMean", "TR.NetProfitMedian", "TR.DPSSmartEst",
              "TR.DPSMean", "TR.DPSMedian", "TR.EpsSmartEst", "TR.EPSMean", "TR.EPSMedian", "TR.PriceSalesRatioSmartEst",
              "TR.PriceSalesRatioMean", "TR.PriceSalesRatioMedian", "TR.PriceMoRegionRank", "TR.SICtryRank", "TR.EQCountryListRank1_Latest",
              "TR.CreditComboRegionRank", "TR.CreditRatioRegionRank", "TR.CreditStructRegRank", "TR.CreditTextRegRank",
              "TR.TRESGScoreGrade(Period=FY0)", "TR.TRESGCScoreGrade(Period=FY0)", "TR.TRESGCControversiesScoreGrade(Period=FY0)"
             ]

    # Open and retrieve instruments
    if get_universe():
        universe = INSTRUMENTS_LIST[:DEFAULT_UNIVERSE_SIZE]
        
    return len(universe) > 0


def get_universe():
    global INSTRUMENTS_LIST
    
    try:
        with open(INSTRUMENTS_FILE, "r") as myInstrumentFile:
            for readedLine in myInstrumentFile:
                instrument = readedLine.replace('\n', '')
                if (instrument):
                    INSTRUMENTS_LIST.append(instrument)
            print(f"Processed instrument list containing: {len(INSTRUMENTS_LIST)} items.")
    except Exception as e:
        print(f"Issue opening the file '{INSTRUMENTS_FILE}'\n{e}")  

    return len(INSTRUMENTS_LIST) > 0

def execute(processor, batch_size, workers):
    print(f'Launching {algorithm.name}...')
    start_time = time.time()
    
    if algorithm == Algorithm.NATIVE:
        df = processor.internal_get_data(universe=universe, fields=fields)
    elif algorithm == Algorithm.THREAD:
        df = processor.threadPoolExecutor_get_data(universe=universe, fields=fields, nb_items_threshold=batch_size)
    elif algorithm == Algorithm.PROCESS:
        df = processor.processPoolExecutor_get_data(universe=universe, fields=fields, 
                                                    nb_items_threshold=batch_size, nb_workers=workers)
    else:
        df = processor.hybridPoolExecutor_get_data(universe=universe, fields=fields, 
                                                   nb_items_threshold=batch_size, nb_workers=workers)
            
    print(f"{algorithm.name} execution finished: --- {time.time()-start_time} seconds elapsed ---")

if __name__ == "__main__":
    if init():
        main_menu()
    exit()
