from dataclasses import dataclass

# The Data Library for Python. 
import refinitiv.data as rd

# Popular container/dataframe to capture and manipulate data we extract
import pandas as pd

# some basic libraries 
from typing import Union
import time
import math
from datetime import datetime

# Libraries form multithreading/multiprocessing
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
       
   
@dataclass
class DataProcessor:
    """The DataProcessor provides a collection of methods that define multiple ways to execute the retrieval of data
       using LSEGs Data Library for Python.  Each method is designed to retrieve data using the popular 'get_data()'
       mechanism using a large set of items using different concurrent mechanisms to measure performance.
    """
    
    # Default maximum number of items per batch
    NB_ITEMS_THRESHOLD = 800
    
    # When testing the hybrid approach, span the threads across multiple child processes
    CHILD_PROCESSES = 2

    # Embed the rd.get_data function
    # Define a defensive get_data function to retrieve data from Workspace
    def internal_get_data(self, 
                          universe: Union[str, list],
                          fields: Union[str, list] = [],
                          parameters: Union[str,dict] = {},
                          open_the_session = False) -> pd.DataFrame:
        """Defines the native mechanism to pull data from the backend.  In this scenario, a group of items and the fields of interest
           are provided to the 'get_data()' function within the Data Library for Python.  We rely on the optimizations natively 
           provided by the library.
        """         
        try:
            if open_the_session:
                self.__open_session()
                
            res = pd.DataFrame()
            nb_relaunch_request = 5
            succeed = False

            current_time = datetime.now()
            formatted_time = current_time.strftime('%H:%M:%S.%f')[:-3]
            print(f'Batch of {1 if isinstance(universe, str) else len(universe)} requested at: {formatted_time}\n', end='')
            
            while (succeed is False and nb_relaunch_request > 0):
                try:
                    res = rd.get_data(universe, fields, parameters)
                except Exception as ex:
                    nb_relaunch_request = nb_relaunch_request - 1
                    time.sleep(0.2)
                    print(f'Failed to execute data request: {ex}\n{nb_relaunch_request} attempt(s) remaining...');
                else:
                    succeed = True

            if open_the_session:
                self.close()
        
            return res
        except Exception as e:
            print(f'internal_get_data exception: {e}')

    # rd.get_data with a pool of threads
    def threadPoolExecutor_get_data(self,
                                    universe: Union[str, list],
                                    fields: Union[str, list] = [],
                                    parameters: Union[str,dict] = {},
                                    nb_items_threshold = NB_ITEMS_THRESHOLD,
                                    open_the_session = False) -> pd.DataFrame:
        """Using the threading features defined within the Python concurrent library, an attempt is made to break apart the
           request into optimized chunks to be executed across multiple threads.
        """
        if open_the_session:
            self.__open_session()

        output_df = pd.DataFrame()

        current_time = datetime.now()
        formatted_time = current_time.strftime('%H:%M:%S.%f')[:-3]
        current_process = multiprocessing.current_process()

        with ThreadPoolExecutor(max_workers = None) as executor: 
            best_nb_items_threshold = max(math.ceil(len(universe) / executor._max_workers), nb_items_threshold)
            universe_list = self.__split_list_into_chunks(universe, best_nb_items_threshold)
            nb_loop = len(universe_list)
            print(f"Process ID:{current_process.pid} ==> sending batches with max size of {best_nb_items_threshold} on {nb_loop} threads at {formatted_time}\n", end='')

            try:
                for result in executor.map(self.internal_get_data,
                                           universe_list,
                                           [fields] * nb_loop,
                                           [parameters] * nb_loop):
                    output_df = pd.concat([output_df, result], ignore_index=True)
            except Exception as e:
                print(f'ThreadPoolExecutor_get_data exception {e}')

        if open_the_session:
            self.close()
            
        return output_df


    # rd.get_data with a pool of processes
    def processPoolExecutor_get_data(self,
                                     universe: Union[str, list],
                                     fields: Union[str, list] = [],
                                     parameters: Union[str,dict] = {},
                                     nb_items_threshold = NB_ITEMS_THRESHOLD,
                                     nb_workers = CHILD_PROCESSES) -> pd.DataFrame:
        """Using the threading features defined within the Python concurrent library, an attempt is made to break apart the
           request into multiple child processes to be executed in parallel to the backend in a single thread.
        """        
        output_df = pd.DataFrame()
        
        with ProcessPoolExecutor(max_workers = nb_workers) as executor:
            best_nb_items_threshold = math.ceil(len(universe) / executor._max_workers)            
            universe_list = self.__split_list_into_chunks(universe, max(best_nb_items_threshold, nb_items_threshold))
            nb_loop = len(universe_list)
            
            if nb_loop < 2:
                return self.internal_get_data(universe=universe, fields=fields)
                                
            print(f"Launching process execution using {nb_loop} child processes...")

            try:
                for result in executor.map(self.internal_get_data,
                                           universe_list,
                                           [fields] * nb_loop,
                                           [parameters] * nb_loop,
                                           [True] * nb_loop):
                    output_df = pd.concat([output_df, result], ignore_index=True)
            except Exception as e:
                print(f'ProcessPoolExecutor_get_data exception: {e}')

        return output_df

    # rd.get_data using a hybrid pool of processes/threads
    def hybridPoolExecutor_get_data(self,
                                    universe: Union[str, list],
                                    fields: Union[str, list] = [],
                                    parameters: Union[str,dict] = {},
                                    nb_items_threshold = NB_ITEMS_THRESHOLD,
                                    nb_workers = CHILD_PROCESSES) -> pd.DataFrame:
        """Using the threading features defined within the Python concurrent library, an attempt is made to break apart the
           request into multiple child processes to be executed in parallel to the backend across multiple threads. This
           feature is a hybrid of the process and threading capabilities offered by Python.
        """
        output_df = pd.DataFrame()
        current_time = datetime.now()
        formatted_time = current_time.strftime('%H:%M:%S.%f')[:-3]  

        with ProcessPoolExecutor(max_workers = nb_workers) as executor:
            best_nb_items_threshold = math.ceil(len(universe) / executor._max_workers)            
            universe_list = self.__split_list_into_chunks(universe, max(best_nb_items_threshold, nb_items_threshold))
            nb_loop = len(universe_list)
            
            if nb_loop < 2:
                return self.threadPoolExecutor_get_data(universe=universe, fields=fields, parameters=parameters,
                                                        nb_items_threshold=nb_items_threshold)
            print(f"Launching process execution using {nb_loop} child processes...")
            
            try:
                for result in executor.map(self.threadPoolExecutor_get_data,
                                           universe_list,
                                           [fields] * nb_loop,
                                           [parameters] * nb_loop,
                                           [nb_items_threshold] * nb_loop,
                                           [True] * nb_loop):
                    output_df = pd.concat([output_df, result], ignore_index=True)
            except Exception as e:
                print(f'ProcessPoolExecutor_get_data exception: {e}')

        return output_df

    # Get current session state
    @property
    def __session_state(self):
        return rd.session.get_default().open_state

    # Check if session if opened
    def is_session_opened(self):
        return self.__session_state == rd.OpenState.Opened

    # Close the session
    def close(self):
        rd.close_session()        
    
    # splits the list of items into chunk with a maximum of max_items in a chunk
    def __split_list_into_chunks(self, items, nb_chunks):
        n = max(1, nb_chunks)
        return list(items[i:i+n] for i in range(0, len(items), n))
   
    def __open_session(self):
        config = rd.get_config()
        config.set_param("http.request-timeout", 120)
        rd.open_session()
        
    def __init__(self):
        print("Opening desktop session...")
        self.__open_session()
        print(f'Desktop Session State: {self.__session_state}')
        