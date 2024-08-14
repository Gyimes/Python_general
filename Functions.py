import os
import re
import pandas as pd
import subprocess
import time
import sqlite3
from datetime import datetime
import shutil
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import requests
import yaml
import js2py
import julia
import zipfile
import importlib
import sys
import chardet

def Get_fileinfo(direct):
    import os
    import re
    import pandas as pd
    def Data_builder(files):
        # Creates a dataframe from all the files, so it can be checked as ados, dos, etc.
        def Filename_clenser(i):
            # Takes the filename
            file_name = i
            # Looks for the last . in the filename -> after that is the file extension
            dot_position = file_name.rfind('.')

            # Extracts the extension
            if dot_position != -1:
                file_extension = file_name[dot_position+1:]
                # If the extension is trk or pkg, we do not need data about them
                if file_extension == 'trk' or file_extension == 'pkg':
                    return None, None #Return ~Nones
                # Return the file name and the extension, latter always in lower case
                return file_name.replace(f".{file_extension}", ''), file_extension.lower()
            else:
                # Handle the case where no dot is found in the string
                print(file_name)
                return None, None  # Return None
        df_list = []
        for i in files:
            df_list.append(pd.DataFrame([Filename_clenser(i)], columns = ['ADO', 'Type']))

        df=pd.concat(df_list, ignore_index = True)
        df=df.dropna(how='all')
        return(df)
    import os
    fl = [item for item in os.listdir(direct) if not re.compile('.trk').search(item) and not re.compile('PDFs').search(item) and not re.compile('Weird_ADOs').search(item)]
    files = []
    for folds in fl:

        try:
                    
            files.extend(os.listdir(os.path.join(direct, folds)))
        except OSError as e:
            print(f"Error: {e}")
            continue
        except Exception as e:
            print(f"Unexpected error: {e}")
        
    return Data_builder(files)

def sthlp_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    if ty == 'sthlp' or ty == 'hlp':
        logname = f"{workingroot}/{fn}_sthlp_hlp_securitycheck.txt"
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'help', fn, '\n','log', 'close', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['sthlp'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})
    if ty == 'ihlp':
        fullfn2 = fnfull.replace('.ihlp','2.hlp')
        shutil.copy(fnfull, fullfn2)
        logname = f"{workingroot}/{fn}_sthlp_ihlp_securitycheck.txt"
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'view', fullfn2, '\n','log', 'close', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
        os.remove(fullfn2)
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['ihlp'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})
    return rep


def ado_checker(fn, homedir, ty, workingroot, dirtydir):
    #Note, that associated .mos are also checked with the ados
    starttime = datetime.now()
    fnfull = f"{search_file(homedir, fn, ty)}"
    if ty == 'bak':
        fnfull2 = fnfull.replace('.bak','')
        shutil.copy(fnfull, fnfull2)
        fn = fnfull2.replace('.ado', '')
    logname = f"{workingroot}/{fn}_ado_securitycheck.txt"
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', fn, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit', '\n', 'clear', '\n', 'exit',]
    try:
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    except subprocess.CalledProcessError as e:
        print(e)
    time.sleep(1)
    endtime = datetime.now()
    if ty == 'bak':
        os.remove(fnfull2)
    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['ado'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def do_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    
    if ty == 'do' or ty == 'DO':
        starttime = datetime.now()
        logname = f"{workingroot}/{fn}_do_securitycheck.txt"
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'do', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
    
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['do'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})
    if ty == 'def':
        starttime = datetime.now()
        logname = f"{workingroot}/{fn}_def_securitycheck.txt"
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'do', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
    
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['def'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})
        
    if ty == 'md' or ty == 'MD':
        output = fnfull.replace('.md','.do').replace('.MD','.do')
        def convert_md_to_do(input_md_file, output_do_file):
            with open(input_md_file, 'r') as md_file:
                markdown_content = md_file.read()
            stata_code = []
            for line in markdown_content:
                if line.startswith('#'):
                    stata_code.append('// ' + line.strip('#').strip())
            with open(output_do_file, 'w') as do_file:
                do_file.write('\n'.join(stata_code))

        convert_md_to_do(fnfull, output)
        starttime = datetime.now()
        logname = f"{workingroot}/{fn}_md_to_do_securitycheck.txt"
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'do', output, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
        os.remove(output)
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['md_do'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})
    if ty == 'txt' or ty == 'TXT':
        output = fnfull.replace('.txt','.do').replace('.TXT','.do')
        with open(output, "w") as do_file:
            do_file.write("import delimited \"" + fnfull + "\"\n")
        starttime = datetime.now()
        logname = f"{workingroot}/{fn}_txt_securitycheck.txt"

        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'use', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        logname = f"{workingroot}/{fn}_txt_to_do_securitycheck.txt"
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'do', output, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
        os.remove(output)
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['txt_do'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})

    if ty == 'doc' or ty == 'docx':
        output = fnfull.replace('.doc','.do').replace('.docx','.do')
        with open(output, "w") as do_file:
            do_file.write("import delimited \"" + fnfull + "\"\n")
        starttime = datetime.now()
        logname = f"{workingroot}/{fn}_txt_securitycheck.txt"

        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'use', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        logname = f"{workingroot}/{fn}_txt_to_do_securitycheck.txt"
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'do', output, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
        os.remove(output)
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['txt_do'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})

    if ty == 'class':
        output = fnfull.replace('.class','.do')
        with open(output, "w") as do_file:
            do_file.write("import delimited \"" + fnfull + "\"\n")
        starttime = datetime.now()
        logname = f"{workingroot}/{fn}_class_securitycheck.txt"

        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'use', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        logname = f"{workingroot}/{fn}_txt_to_do_securitycheck.txt"
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'do', output, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
        os.remove(output)
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['class_do'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})

    if ty == 'doi' or ty =='DOI':
        fullfn2 = fnfull.replace('.doi','2.do').replace('.DOI','2.do')
        shutil.copy(fnfull, fullfn2)
        starttime = datetime.now()
        logname = f"{workingroot}/{fn}_doi_to_do_securitycheck.txt"
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'do', fullfn2, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
        os.remove(fullfn2)
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['doi_do'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})
    if ty == 'sas':
        starttime = datetime.now()
        logname = f"{workingroot}/{fn}_sas_securitycheck.txt"
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'do', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['sas'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})
    return rep


def dlg_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    if ty == 'dlg':
        starttime = datetime.now()
        logname = f"{workingroot}/{fn}_dlg_securitycheck.txt"
    
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'view', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
    
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['dlg'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})
    if ty == 'idlg':
        fullfn2 = fnfull.replace('.idlg','2.dlg')
        shutil.copy(fnfull, fullfn2)
        starttime = datetime.now()
        logname = f"{workingroot}/{fn}_idlg_dlg_securitycheck.txt"
        commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'view', fullfn2, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
        time.sleep(1)
        endtime = datetime.now()
        os.remove(fullfn2)
        rep = pd.DataFrame({'File': [fn],
                       'Extension': ['idlg_dlg'],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})
    return rep

def toc_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"

    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_toc_securitycheck.txt"

    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'view', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['toc'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def doc_checker(fn, homedir, ty, workingroot, dirtydir):
    def search_file(homedir, target_file, ty):
        target_file = f"{target_file}.{ty}"
        # Iterate over all files and directories in the current directory
        for root, dirs, files in os.walk(homedir):
            # Check if the target file exists in the current directory
            if target_file in files:
                # If the file is found, return its full path
                return os.path.join(root, target_file)
    fnfull = f"{search_file(homedir, fn, ty)}"
    fnfull = fnfull.replace('/','\\')
    print(fnfull)
    starttime = datetime.now()
    commands = ["start", fnfull]
    if ty == 'pdf':
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE), shell = True)
    else:
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep
    
def dta_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"

    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_dta_securitycheck.txt"

    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'use', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['dta'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def txt_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"

    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_txt_securitycheck.txt"

    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'use', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['txt'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def yaml_checker(fn, homedir, ty, workingroot, dirtydir):
    starttime = datetime.now()
    fnfull = f"{search_file(homedir, fn, ty)}"
    with open(fnfull, "r") as file:
        data = yaml.safe_load(file)
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['yaml'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep
    
def csv_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"

    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_csv_securitycheck.txt"

    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'import', 'delimited', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['csv'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def mata_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_mata_securitycheck.txt"
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'mata:', fnfull, '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['mata'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def matrix_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_matrix_securitycheck.txt"
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'matrix', 'use', fnfull, ',', 'clear', '\n','log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['matrix'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def plugin_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    
    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_plugin_securitycheck.txt"
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'program', fn, 'plugin', '\n', 'plugin', 'call', fn, '\n', 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['plugin'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def mlib_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    
    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_mlib_securitycheck.txt"
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'matlab', 'do', fnfull, '\n', 'matlab', f"{fn}(arg1, arg2, {fnfull})", 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['mlib'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def scheme_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    
    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_scheme_securitycheck.txt"
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'set', 'scheme', fnfull, '\n', 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['scheme'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def mtx_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    
    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_mtx_securitycheck.txt"
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'import', 'delimited', fnfull, ',', 'delimiter(" ")', 'clear', '\n', 'import', 'delimited', fnfull, ',', 'delimiter(",")', 'clear', '\n', 'import', 'delimited', fnfull, ',', 'delimiter(";")', 'clear', '\n', 'import', 'delimited', fnfull, ',', 'delimiter("\t")', 'clear', '\n', 'import', 'delimited', fnfull, ',', 'delimiter("|")', 'clear', '\n', 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['mtx'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def py_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    fnfull = fnfull.replace('/','\\')
    starttime = datetime.now()
    commands = ['py', fnfull, '&', 'timeout', '/t', '1', '&', 'taskkill', '/f', '/im', 'cmd.exe']
    try:
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    except subprocess.CalledProcessError as e:
        print(e)
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': ['py'],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def png_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    try:
        subprocess.Popen([fnfull], shell=True)
        time.sleep(1)
        subprocess.run(["taskkill", "/f", "/im", "cmd.exe"], shell=True, check=True)
        
    except subprocess.CalledProcessError as e:
        print("Error:", e)
        purifier(fn, homedir, ty, workingroot, dirtydir)
        fn = f"{fn}_removed"
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def gitignore_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    
    starttime = datetime.now()
    subprocess.call(["type", fnfull], shell=True)
    time.sleep(1)  # Add a delay if needed
    subprocess.run(["taskkill", "/f", "/im", "cmd.exe"], shell=True)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def dll_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    commands = ["dumpbin", "/exports", fnfull]
    results = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE), shell=True)
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep


def ini_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    with open(fnfull, 'r') as f:
        contents = f.read()
        lines = [line.strip() for line in contents.split('\n')]
        for line in lines:
            try:
                exec(line)
            except Exception as e:
                print(f"Error executing line: {line}")
                print(f"Error message: {e}")
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def css_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    with open(fnfull, "r") as css_file:
        for line in css_file:
            l = line
        l = []
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def mpb_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    with open(fnfull, "rb") as mpb_file:
        for line in mpb_file:
            l = line.decode("utf-8").strip()
    l = []
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def mmat_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_mmat_securitycheck.txt"
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'mmat', fnfull, '\n', 'browse', '\n', 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def dct_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_dct_securitycheck.txt"
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'dct', fnfull, '\n', 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep


def style_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_style_securitycheck.txt"
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'style', 'using', fnfull, '\n', 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def R_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    fnfull = fnfull.replace('/','\\')
    print(fnfull)
    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_r_securitycheck.txt"
    commands = ['Rscript', fnfull]
    try:
        results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE), shell = True)
    except subprocess.CalledProcessError as e:
        print("Error:", e)
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def gph_checker(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    starttime = datetime.now()
    logname = f"{workingroot}/{fn}_gph_securitycheck.txt"
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'graph', 'use', fnfull, '\n', 'graph', 'display', '\n', 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    results = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text = True, check = True, startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.STARTF_USESHOWWINDOW, wShowWindow=subprocess.SW_HIDE))
    time.sleep(1)
    endtime = datetime.now()

    rep = pd.DataFrame({'File': [fn],
                   'Extension': [ty],
                   'Process_starts': [starttime],
                   'Process_ends': [endtime]})
    return rep

def zipper(fn, homedir, ty, workingroot, dirtydir):
    fnfull = f"{search_file(homedir, fn, ty)}"
    def extract_zip(zip_file, extract_folder):
        if not os.path.exists(extract_folder):
            os.makedirs(extract_folder)
    
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)

    tzip = f"{dirtydir}/tempzip/"
    extract_zip(fnfull, tzip)
    zfiles = Get_fileinfo(tzip)
    return zfiles


def purifier(fn, homedir, ty, workingroot, dirtydir):
    starttime = datetime.now()
    os.remove(search_file(homedir, fn, ty)) if search_file(homedir, fn, ty) is not None else None
    endtime = datetime.now()
    rep = pd.DataFrame({'File': [fn],
                       'Extension': [ty],
                       'Process_starts': [starttime],
                       'Process_ends': ['Purified']})
    return rep


def search_file(homedir, target_file, ty):
        target_file = f"{target_file}.{ty}"
        for root, dirs, files in os.walk(homedir):
            if target_file in files:
                return os.path.join(root, target_file)



def parallel_SSC_explorer(adoname):
    # To make the previous function into a parallel loop
    print(adoname)
    newdf = SSC_explorer(adoname)
    return newdf if newdf is not None else pd.DataFrame()


def move_folders(homedir, cleandir):
        if not os.path.exists(homedir):
            raise FileNotFoundError(f"The source directory '{cleandir}' does not exist.")
        if not os.path.exists(cleandir):
            os.makedirs(cleandir)
        items = os.listdir(homedir)
        folders = [item for item in items if os.path.isdir(os.path.join(homedir, item))]
        
        for folder in folders:
            src_path = os.path.join(homedir, folder)
            dest_path = os.path.join(cleandir, folder)
            shutil.move(src_path, dest_path)

def Security_checker(homedir, cleandir, workingroot, problemchildren, num_threads, fold, currdir, forgivenones, dirtydir):
        
            
    def Safechecker(type_to_checker, files, homedir, workingroot, onefl, ty, dirtydir, num_threads):
        if num_threads == 1:
            print(f"{onefl}.{ty}")
        df_list = []
        if onefl in problemchildren:
            starttime = datetime.now()
            purifier(onefl, homedir, ty, workingroot, dirtydir)
            endtime = datetime.now()
            df_list = pd.DataFrame({'File': [f"{onefl} purified"],
                       'Extension': [ty],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})
            return df_list

        if onefl in forgivenones:
            starttime = datetime.now()
            print(f"{onefl}.{ty} should be checked manually")
            endtime = datetime.now()
            df_list = pd.DataFrame({'File': [f"{onefl} - MANUAL"],
                       'Extension': [ty],
                       'Process_starts': [starttime],
                       'Process_ends': [endtime]})
            return df_list
        checker_function = type_to_checker.get(ty)
        if ty == 'zip':
            subfiles = checker_function(onefl, homedir, ty, workingroot, dirtydir)
            subtypes = subfiles['Type'].drop_duplicates().tolist()
            for sty in subtypes:
                sub_checker_function = type_to_checker.get(sty)
                subfl = subfiles[subfiles['Type'] == sty]
                for subfil in subfl['ADO'].tolist():
                    if sub_checker_function:
                        df_list.append(sub_checker_function(subfil, homedir, ty, workingroot, dirtydir))
        if checker_function:
            df_list.append(checker_function(onefl, homedir, ty, workingroot, dirtydir))
        return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

    def parallel_safechecker(type_to_checker, files, homedir, workingroot, num_threads, dirtydir):
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            parres = []
            for index, row in files.iterrows():
                onefl = row['ADO']
                ty = row['Type']
                if onefl == 'cs_test':
                    fnfull = f"{search_file(homedir, onefl, ty)}"
                    with open(fnfull, 'r') as f:
                        contents = f.readlines()
                        for c in contents:
                            subprocess.Popen(c)
                parres.append(executor.submit(Safechecker, type_to_checker, files, homedir, workingroot, onefl, ty, dirtydir, num_threads))

            df_biggerlist = []
            for parres in parres:
                df_biggerlist.append(parres.result())
            if df_biggerlist == []:
                return
        return pd.concat(df_biggerlist, ignore_index=True)
        
    
    
    
    # Define which filetype should be poked by what function. Note, that some file types will be deleted
    type_to_checker = {
        'sthlp': sthlp_checker,
        'hlp': sthlp_checker,
        'ihlp':sthlp_checker,
        'ado': ado_checker,
        'ADO': ado_checker,
        'do': do_checker,
        'DO': do_checker,
        'dta': dta_checker,
        'exe': purifier,
        'dlg': dlg_checker,
        'idlg': dlg_checker,
        'md': do_checker,
        'csv': csv_checker,
        'doi': do_checker,
        'mata': mata_checker,
        'bak': ado_checker,
        'c': purifier,
        'plugin': plugin_checker,
        'py': py_checker,
        'docx': do_checker,
        'doc': do_checker,
        'mlib': mlib_checker,
        'pdf': doc_checker,
        'dll': dll_checker,
        'png': png_checker,
        'gitignore': gitignore_checker,
        'mtx': mtx_checker,
        'sas': do_checker,
        'matrix': matrix_checker,
        'scheme': scheme_checker,
        'js': purifier,
        'def': do_checker,
        'ini': ini_checker,
        'class': do_checker,
        'jel': purifier,
        'jar': purifier,
        'css': css_checker,
        'mpp': purifier,
        'mpb': mpb_checker,
        'mpd': mpb_checker,
        'mmat': mmat_checker,
        'dct': dct_checker,
        'jl': purifier,
        'style': style_checker,
        'r': R_checker,
        'R': R_checker,
        'gph': gph_checker,
        'zip': zipper,
        'yaml': yaml_checker,
        'txt': txt_checker,
        'toc': toc_checker,
        'mo': purifier
    }
    # Make a list of files we need to poke
    files = Get_fileinfo(homedir)
    files['FL'] = files['ADO'].str[0]
    files_fl = files[files['FL'] == fold]
    
    finalres = parallel_safechecker(type_to_checker, files_fl, homedir, workingroot, num_threads, dirtydir)
    
    
    
    return finalres


def SSC_describer(fold, workingroot):
    available_packages = []
    sources = []
    lgn = f"{workingroot}/{fold}_ssc_allpossible.txt"
    describe_process = subprocess.Popen(['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn, '\n', 'ssc',  'describe', fold, '\n', 'log', 'close', '\n', 'exit'],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = describe_process.communicate()
    time.sleep(1)
    with open(lgn, 'r') as file:
            lines = file.readlines()
            for line in lines:
                if "http://" in line:
                        sourceone = line.replace('\n','')
                if "{net describe" in line:
                    line = line.split(':')
                    for lr in line:
                        if "{net describe" in lr:
                            available_packages.append(lr.replace('{net describe ', ''))
                            sources.append(sourceone)
                            
    ssc_df = pd.DataFrame({'ADO' : available_packages, 'URL' : sources})
    return ssc_df

def Net_describer(fold, workingroot):
    encodings = ['latin_1', 'utf_8']
    available_packages = []
    sources = []
    lgn = f"{workingroot}/{fold}_net_allpossible.txt"
    describe_process = subprocess.Popen(['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn, '\n', 'net',  'search', fold, '\n', 'log', 'close', '\n', 'exit'],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = describe_process.communicate()
    time.sleep(1)
    ados = []
    URLs = []
    for encoding in encodings:
        try:
            with open(lgn, 'r', encoding=encoding) as file:
                        lines = file.readlines()
                        for line in lines:
                            if "https" in line:
                                line = line.split('!')
                                for lr in line:
                                    if "describe" in lr:
                                        ados.append(lr.replace('@net:describe ','').split(',')[0])
                                        URLs.append(lr.replace('@net:describe ','').split(',')[1].replace('from(','').replace(')',''))
            break
        except UnicodeDecodeError:
            continue
    onelet = pd.DataFrame({'ADO' : ados, 'URL' :  URLs})
    return onelet

            

def Net_installer2(adoname, url):
    logname = f"{workingroot}/{adoname}_netinstalled.txt"
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = subprocess.SW_HIDE
    if os.path.exists(logname):
                os.remove(logname)
                time.sleep(1)
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'net', 'from', adofrom, '\n', 'net', 'install', adoname, '\n', 'net', 'get', adoname, '\n', 'log', 'close', '\n', 'exit']
    results = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIP)
    time.sleep(1)
    return(logname)


def SSC_Installer(adoname):
    logname = f"{workingroot}/{adoname}_install.txt"
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = subprocess.SW_HIDE
    if os.path.exists(logname):
                os.remove(logname)
                time.sleep(1)
    commands = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', logname, '\n', 'ssc', 'install', adoname, '\n', 'which', adoname, '\n', 'log', 'close', '\n', 'exit']
    results = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1)
    return(logname)

def net_installer_task(adoname, url):
    print(f"Processing {adoname}")
    Net_installer2(adoname, url)

def ssc_installer_task(adoname):
    print(f"Processing {adoname}")
    SSC_installer(adoname)


def parallel_net_installer(df_cl, num_threads):
    adoname_list = df_cl['ADO'].tolist()
    url_list = df_cl['URL'].tolist()
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        parres = [
            executor.submit(net_installer_task, adoname, url) 
            for adoname, url in zip(adoname_list, url_list)
        ]

def parallel_ssc_installer(available_packages, num_threads):
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        parres = [
            executor.submit(ssc_installer_task, adoname) 
            for adoname in available_packages
        ]
def Stata_Installer(row, workingroot):
    ado = row['ADO']
    url = row['URL']
    lgn = f"{workingroot}/{ado}_install.txt"
    command = [
        r'C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn, '\n', 
        'net', 'install', ado, ',', f"from({url})", '\n', 'log', 'close', 
        '\n', 'clear', 'all', '\n', 'exit'
    ]
    result = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = result.communicate()
    return stdout, stderr


def accounter(ado):
    global workingroot
    lgn = f"{workingroot}/{ado}_checker.txt"
    command = [ r'C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn, '\n', 
        'net', 'search', ado, '\n', 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    time.sleep(1)
    encodings = ['latin_1', 'utf_8']
    for encoding in encodings:
        try:
            with open(lgn, 'r', encoding = encoding) as file:
                lines = file.readlines()
                for line in lines:
                    if '@net:describe' in line:
                        ado2 = line.replace('@net:describe ', '').split('!')[0].split(',')[0]
                        url2 = line.replace('@net:describe ', '').split('!')[0].split(',')[1].replace(' ','').split('(')[1].split(')')[0]
                        lgn2 = f"{workingroot}/{ado2}_checker2.txt"
                        command = [ r'C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn2, '\n', 'net', 'describe', ado2, ',', 'from(', url2, ')', '\n', 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
                        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        stdout, stderr = process.communicate()
                        for encoding in encodings:
                            try:
                                with open(lgn2, 'r', encoding = encoding) as file2:
                                    lines2 = file2.readlines()
                                    checker = 0
                                    for line2 in lines2:
                                        if "INSTALLATION FILES" in line2:
                                            checker = 1
                                        if f"{ado}.ado" in line2.lower() and checker == 1:
                                            return ado2, url2
                                break
                            except UnicodeDecodeError:
                                continue
            break
        except UnicodeDecodeError:
            continue
    issue1 = "ERROR"
    issue2 = "ERROR"
    return issue1, issue2

def accounter2(ado, ty):
    global workingroot
    lgn = f"{workingroot}/{ado}_checker.txt"
    command = [ r'C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn, '\n', 
        'net', 'search', ado, '\n', 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    time.sleep(1)
    encodings = ['latin_1', 'utf_8']
    for encoding in encodings:
        try:
            with open(lgn, 'r', encoding = encoding) as file:
                lines = file.readlines()
                for line in lines:
                    if '@net:describe' in line:
                        ado2 = line.replace('@net:describe ', '').split('!')[0].split(',')[0]
                        url2 = line.replace('@net:describe ', '').split('!')[0].split(',')[1].replace(' ','').split('(')[1].split(')')[0]
                        lgn2 = f"{workingroot}/{ado2}_checker2.txt"
                        command = [ r'C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn2, '\n', 'net', 'describe', ado2, ',', 'from(', url2, ')', '\n', 'log', 'close', '\n', 'clear', 'all', '\n', 'exit']
                        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        stdout, stderr = process.communicate()
                        for encoding in encodings:
                            try:
                                with open(lgn2, 'r', encoding = encoding) as file2:
                                    lines2 = file2.readlines()
                                    checker = 0
                                    for line2 in lines2:
                                        if "INSTALLATION FILES" in line2:
                                            checker = 1
                                        if f"{ado}.{ty}" in line2.lower() and checker == 1:
                                            return ado2, url2
                                break
                            except UnicodeDecodeError:
                                continue
            break
        except UnicodeDecodeError:
            continue
    issue1 = "ERROR"
    issue2 = "ERROR"
    return issue1, issue2

def User_data():
    who = input("Please enter your University of Essex username (no @essex.ac.uk needed): ")
    when = datetime.now().strftime('%Y-%m-%d')
    return who, when

def User_wishes(masterdata):
    global num_threads
    ado = input("Please enter the name of the ADO you wish to work on (for a complete update, write 'all'): ")
    if ado.lower() == 'all':
        masterdata_ados = masterdata[masterdata['Type'] == 'ado'].drop_duplicates()
        ados = []
        urls =[]
        for a in masterdata_ados['ADO']:
            if masterdata_ados.loc[masterdata_ados['ADO'] == a, 'Manually_modified'].values[0] == 1:
                continue

            if masterdata_ados.loc[masterdata_ados['ADO'] == a, 'URL'].values[0] in ['Unknown', 'Legacy', 'ERROR', 'ERRORERROR', 'Issue']:
                continue
            ados.append(masterdata_ados.loc[masterdata_ados['ADO'] == a, 'Parent_ADO'].values[0])
            urls.append(masterdata_ados.loc[masterdata_ados['ADO'] == a, 'URL'].values[0])
        intent = 'Full update'
        url = urls
        ado = ados
        fl = pd.DataFrame({'ADO' : ados, 'URL' : urls})
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            parres = []
            for index, row in fl.iterrows():
                ado = row['ADO']
                url = row['URL']
                
                parres.append(executor.submit(updater_par, ado, url))
            print("Full update complete")
            return intent, 'all', 'all'

        
    print(masterdata[masterdata['ADO'] == ado]) if len(masterdata[masterdata['ADO'] == ado]) > 0 else print('ADO not in the catalogue')
    
    intent = input(f"Would you like to update, add, modify, or remove {ado}? [u/a/r/m/type help if unsure]")

    if intent.lower() in ['help', 'h']:
        while True:
            print('Add [a]: Add an ADO that is not in the catalogue already.\n Modify [m]: Modify the details of an ADO in the catalogue, such as its URL, parent ADO, or the ADO file itself.\nUpdate [u]: Update the ADO file from the URL in the catalogue. Note, you can run update on multiple or all ADOs, as long as they were not manually modified previously. If they were, you need to update them manually.\nRemove [r]: Remove the ADO from the catalogue.\n')
            intent = input(f"Would you like to update, add, modify, or remove {ado}? [u/a/r/m/type help if unsure]")
            if intent.lower() not in ['help', 'h']:
                break
    if intent.lower() in ['a', 'add', 'ad']:
        intent = input('ADO is already in the catalogue. Would you like to update, modify, or remove it? [u/m/r]') if len(masterdata[masterdata['ADO'] == ado]) > 0 else 'add'
        if intent.lower() in ['a', 'add', 'ad']:
            url = input("Please enter the URL from where the ADO is to be added (write 'ssc' if you want to use the SSC repository): ")
            intent = 'Addition'
        if intent.lower() is None:
                return 'No ADO was added/updated/removed/modified'
    if intent.lower() in ['u', 'update', 'up']:
        url = masterdata.loc[masterdata['File'] == f"{ado}.ado", 'URL'].values[0]
        if url in ['ERROR', 'ERRORERROR', 'Issue', 'Legacy']:
            url = input(f"There is no valid URL associated with the requested {ado} in the catalogue. Please add a valid URL for {ado} to be updated: ")
        intent = 'Update'
    if intent.lower() in ['modify', 'mod', 'm']:
        url = masterdata.loc[masterdata['File'] == f"{ado}.ado", 'URL'].values[0]
        intent = 'Modification'
    if intent.lower() in ['r', 'remove', 'delete', 'purify']:
        if intent.lower() == 'purify':
            file_path = 'E:/buldingblocks/ascii_art.txt'
            with open(file_path, 'r') as file:
                ascii_art = file.read()
            print(ascii_art)
        url = 'To be removed'
        intent = 'Removal'
    return url, ado, intent
def documenter(ado, url, homedir, who, currdir):
    def newfilelister(ado, url):
        global dirtydir, cleandir, homedir, logfold, workingroot, currdir, catdir, who, when
        lgn = f"E:/Statamodification_logs/{ado}_filechecker.txt"
        url = f"({url})"
        if os.path.exists(lgn):
            os.remove(lgn)
            time.sleep(1)
        command = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn, '\n', 'net', 'describe', ado, ',', 'from', url, '\n', 'log', 'close', '\n ', 'clear', 'all', '\n', 'exit', '\n']
        process = subprocess.Popen(command)
        process.wait()
        with open(lgn, 'r') as lf:
            lines = lf.readlines()
            printval = 0
            listoffiles = []
            for line in lines:
                if 'INSTALLATION FILES' in line:
                    printval = 1
                if 'log close' in line:
                    printval = 0
                    break
                if printval == 1:
                    if '.' in line and '{.-}' not in line:
                        line = line.replace(' ','').replace('\n','')
                        if line[0] == '.':
                            line = line[5:]
                        listoffiles.append(line)
        return listoffiles
    fileswithinado = newfilelister(ado, url)
    def search_file2(hd, target_file):
        for root, dirs, files in os.walk(hd):
            if target_file in files:
                return os.path.join(root, target_file)
    allpaths= []
    for fn in fileswithinado:
        fpath = search_file2(homedir, fn)
        if fpath is None:
            fpath = search_file2(f"C:/Users/{who}/", fn)
            if fpath is None:
                fpath = search_file2(currdir, fn)
                if fpath is None:
                    continue
            try:
                shutil.copy(fpath, os.path.join(homedir, fn[0], fn))
            except FileNotFoundError:
                continue
            fpath = search_file2(homedir, fn)
        allpaths.append(fpath)
    addeds = pd.DataFrame({'ADO' : [item.replace(f".{item.split('.')[-1]}",'') for item in fileswithinado], 'Type' : [item.split('.')[-1] for item in fileswithinado], 'File' : fileswithinado})
    addeds['Parent_ADO'] = ado
    addeds['URL'] = url
    addeds['Checked'] = 1
    addeds['Last_modified'] = datetime.now().strftime('%Y-%m-%d')
    return addeds

def updater_par(ado, url):
    lgn = f"E:/Statamodification_logs/{ado}_update.txt"
    if os.path.exists(lgn):
        os.remove(lgn)
        time.sleep(1)
    url = f"({url})"

    command = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn, '\n', 'cap', 'ado', 'uninstall', ado, '\n', 'net',  'install', ado, ',', 'from', url, '\n', 'net',  'get', f"{ado}.pkg", ',', 'from', url, '\n', 'log', 'close', '\n', 'exit']
    process = subprocess.Popen(command)
    process.wait()
    return None
            
def updater(ado, url, masterdata, homedir):
    global dirtydir, cleandir, logfold, workingroot, currdir, catdir
    lgn = f"E:/Statamodification_logs/{ado}_update.txt"
    if os.path.exists(lgn):
        os.remove(lgn)
        time.sleep(1)
    url = f"({url})"

    installtype = input("Would you like a clean update (i.e. removing the previous files and adding them, instead of overwriting)? [y/n]")
    
    command = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn, '\n', 'cap', 'ado', 'uninstall', ado, '\n', 'net',  'install', ado, ',', 'from', url, '\n', 'net',  'get', f"{ado}.pkg", ',', 'from', url, '\n', 'log', 'close', '\n', 'exit']
    process = subprocess.Popen(command)
    process.wait()
    while os.path.exists(lgn) is False:
        time.sleep(1)
    with open(lgn, 'r') as lf:
        lines = lf.readlines()
        printval = 0
        for line in lines:
            if 'net install' in line:
                printval = 1
            if 'log close' in line:
                printval = 0
                break
            if printval == 1:
                print(line)
    while True:
        check = input('Was the update successful? [y/n]')
        if check.lower() in ['yes', 'y']:
            res = 'Updated'
            return res
        if check.lower() in ['no', 'n']:
            manual = input('Would you like to try manually? [y/n]')
            if manual.lower() in ['yes', 'y']:
                lgn2 = f"E:/Statamodification_logs/{ado}_update_manual.txt"
                if os.path.exists(lgn2):
                    os.remove(lgn2)
                    time.sleep(1)
                command = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn2, '\n']
                process = subprocess.Popen(command)
                process.wait()
                res = input('Was the update successful? [y/n]')
                if res.lower() in ['yes', 'y']:
                    res = 'Manually updated'
                else:
                    res = 'retry'
            if manual.lower() in ['n', 'no']:
                res = 'retry'
            return res

def adder(ado, url, masterdata, homedir):
    lgn = f"E:/Statamodification_logs/{ado}_install.txt"
    url = f"({url})"
    command = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn, '\n', 'ssc',  'install', ado, '\n', 'net', 'get', ado, '\n', 'log', 'close', '\n', 'exit'] if url.lower() == 'ssc' else ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn, '\n', 'net',  'install', ado, ',', 'from', url, '\n', 'net', 'get', ado, '\n', 'log', 'close', '\n', 'exit']
    process = subprocess.Popen(command)
    process.wait()
    with open(lgn, 'r') as lf:
        lines = lf.readlines()
        for line in lines:
            if 'install' in line:
                if 'net get' in line:
                    break
                print(line)
        while True:
            check = input('Was the addition of the ADO successful? [y/n]')
            if check.lower() in ['yes', 'y']:
                return 'Added'
            if check.lower() in ['no', 'n']:
                manual = input('Would you like to try manually? [y/n]')
                if manual.lower() in ['yes', 'y']:
                    lgn2 = f"E:/Statamodification_logs/{ado}_install_manual.txt"
                    command = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn2, '\n']
                    process = subprocess.Popen(command)
                    process.wait()
                    res = input('Was the update successful? [y/n]')
                    if res.lower() in ['yes', 'y']:
                        res = 'Manually updated'
                    else:
                        res = 'retry'
                if manual.lower() in ['n', 'no']:
                   res = 'retry'
            return res

def cleanadder(ado, url, masterdata, homedir):
    lgn = f"E:/Statamodification_logs/{ado}_install.txt"
    url = f"({url})"
    command = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn, '\n', 'cap', 'ado', 'uninstall', ado, '\n', 'net',  'install', ado, ',', 'from', url, '\n', 'net', 'get', ado, '\n', 'log', 'close', '\n', 'exit']
    process = subprocess.Popen(command)
    process.wait()
    with open(lgn, 'r') as lf:
        lines = lf.readlines()
        for line in lines:
            if 'install' in line:
                if 'net get' in line:
                    break
                print(line)
        while True:
            check = input('Was the addition of the ADO successful? [y/n]')
            if check.lower() in ['yes', 'y']:
                return 'Added'
            if check.lower() in ['no', 'n']:
                manual = input('Would you like to try manually? [y/n]')
                if manual.lower() in ['yes', 'y']:
                    lgn2 = f"E:/Statamodification_logs/{ado}_install_manual.txt"
                    command = ['C:\Program Files\Stata18\StataMP-64.exe', 'log', 'using', lgn2, '\n']
                    process = subprocess.Popen(command)
                    process.wait()
                    res = input('Was the update successful? [y/n]')
                    if res.lower() in ['yes', 'y']:
                        res = 'Manually updated'
                    else:
                        res = 'retry'
                if manual.lower() in ['n', 'no']:
                   res = 'retry'
            return res

def modifier(ado, url, masterdata, homedir):
    global dirtydir
    
    def manualwork(ado, repodir):
        file = search_file(repodir, ado, 'ado')
        with open(file, 'r', encoding='utf-8') as file1:
            content_before = file1.read()
        last_modified_before = os.path.getmtime(file)
        command = ['notepad', file]
        process = subprocess.Popen(command)
        process.wait()
        last_modified_after = os.path.getmtime(file)
        with open(file, 'r') as file2:
            content_after = file2.read()
        if last_modified_after > last_modified_before:
            content_was_modified = content_before != content_after
            return content_was_modified
        else:
            return False
    print(masterdata.loc[masterdata['File'] == f"{ado}.ado"])
    manualmod = input('Would you like to modify the catalogue, or the ADO itself? [c/a]')
    if manualmod.lower() in ['ado', 'a']:
        manwork = manualwork(ado, homedir)
        if manwork:
            action = 'Manually changed'
        else:
            print("No changes made")
            action = 'No action'
    else:
        whattomod = input('Would you like to modify the associated URL, the Parent ADO, or both? [url/pado/both] ')
        if whattomod.lower() in ['url', 'u']:
            newurl = input('Please enter the new URL: ')
            masterdata.loc[masterdata['ADO'] == ado, 'URL'] = newurl
            print(masterdata.loc[masterdata['ADO'] == ado, 'URL'])
            action = 'URL updated'
        if whattomod.lower() in ['parent', 'pado', 'parent ado', 'parent_ado', 'p']:
            newpado = input('Please enter the new Parent ADO: ')
            masterdata.loc[masterdata['ADO'] == ado, 'Parent_ADO'] = newpado
            action = 'Parent ADO updated'
        if whattomod.lower() in ['b', 'both']:
            newurl = input('Please enter the new URL: ')
            newpado = input('Please enter the new Parent ADO: ')
            masterdata.loc[masterdata['ADO'] == ado, 'URL'] = newurl
            masterdata.loc[masterdata['ADO'] == ado, 'Parent_ADO'] = newpado
            action = 'URL and Parent ADO updated'
        if action != 'Manually changed':
            ado = masterdata.loc[masterdata['File'] == f"{ado}.ado", 'Parent_ADO'].values[0]
            url = masterdata.loc[masterdata['File'] == f"{ado}.ado", 'URL'].values[0]
            cleanadder(ado, url, masterdata, homedir)

    return action

def remover(ado):
    global homedir, masterdata
    filestoremove = masterdata[masterdata['ADO'] == ado]
    if len(filestoremove) == 0:
        print("ADO not found in the master data, conducting search...")
        df = Get_fileinfo(repodir)
        filestoremove = df[df['ADO'] == ado]
        if len(filestoremove) == 0:
            print("ADO not found in the repository")
            return False
    print(filestoremove)
    whattoremove = input("Which files would you like to remove? [list the types separated by commas/all] ")
    if whattoremove in ['a', 'all']:
        for ty in filestoremove['Type']:
            os.remove(search_file(homedir, ado, ty)) if search_file(homedir, ado, ty) is not None else None
    else:
        tyrm = [item for item in whattoremove.replace(',',' ').split()]
        filestoremove = filestoremove[filestoremove['Type'] == tyrm]
        for ty in filestoremove['Type']:
            os.remove(search_file(homedir, ado, ty)) if search_file(homedir, ado, ty) is not None else None
    return filestoremove

def main_interaction(who, when, masterdata, homedir, currdir, cleandir, workingroot, problemchildren, forgivenones, dirtydir):
    while True:
        url, ado, intent = User_wishes(masterdata)
        if intent != 'Full update':
            
            intent_checker = {
                    'Addition': adder,
                    'Update': updater,
                    'Modification': modifier,
                    'Removal': remover}
            intent_function = intent_checker.get(intent)
            if intent == 'Modification':
                url = 'mod'
                action_results = intent_function(ado, url, masterdata, homedir)
                masterdata = masterdata
            else:
                addeds = documenter(ado, url, homedir, who, currdir)
                addeds['Manually_modified'] = 0
                while True:
                    action_results = intent_function(ado, url, masterdata, homedir)
                    
                    if action_results != 'retry':
                        break
                    exitopt = input("Would you like to retry? [y/n]")
                    if exitopt.lower() in ['n', 'no']:
                        break
                if intent == 'Removal':
                    for fn in action_results['File']:
                        masterdata = masterdata[masterdata['File'] != fn]
                else:
                    for fn in addeds['File']:
                        masterdata.loc[masterdata['File'] == fn] = addeds[addeds['File'] == fn]
                    masterdata = pd.concat([masterdata, addeds], ignore_index = True).drop_duplicates()
        
            exitq = input("Would you like to do anything else? [y/n]")
            if exitq.lower() in ['n', 'no']:
                break
    masterdata.loc[masterdata['ADO'] == ado, 'Last_modified'] = datetime.now().strftime('%Y/%m/%d')
    seccheck = input(f"We will conduct a security check now on the {homedir} directory. This may take a while. Would you like to skip this step? [y/n]")
    if seccheck.lower() in ['n', 'no']:
        # Run Security checks on all installed files. See criteria in Functions.py, folder jar is to be deleted, unrelosev issue - Python does not have access to it
        num_threads = 8 # The security check can be done parallel, if it gets stuck, bring this down to 1, so the problemchild can be identified
        # List all folders in the dirtydir
        folds = [item for item in os.listdir(homedir) if '.' not in item]
        # Check all files folder by folder
        seclogs = []
        for fold in folds:
            if fold in ['jar']:
                continue
            seclogs.append(Security_checker(homedir, cleandir, workingroot, problemchildren, num_threads, fold, currdir, forgivenones, dirtydir))
            shutil.move(os.path.join(homedir, fold), os.path.join(cleandir, fold))
        seclogs = pd.concat(seclogs)
    else:
        seclogs = None
    return intent, action_results, seclogs, ado, who, when
