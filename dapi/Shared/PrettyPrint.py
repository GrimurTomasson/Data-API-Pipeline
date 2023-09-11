from colorama import Fore, Style

class Pretty:
    MaxLineLength = 160
    Separator = "-" * MaxLineLength
    Indent = 0
    
    @staticmethod
    def get_postfix_line (message) -> str:
        return message + " " + ("-" * (Pretty.MaxLineLength -2 - len (message)))
    
    @staticmethod
    def set_indent (indent:int) -> None:
        Pretty.Indent = indent
        
    @staticmethod
    def add_indent () -> None:
        Pretty.Indent += 1
        
    @staticmethod
    def reduce_indent () -> None:
        Pretty.Indent -= 1

    @staticmethod
    def assemble (value:str, prefixWithIndent:bool=True, separatorBefore:bool=False, separatorAfter:bool=False, color:str=Fore.WHITE, size:int=0, tabCount:int=0, sizeChar:str=' '):        
        originalValue = value
        prefix = Fore.LIGHTBLACK_EX + str(tabCount) + (" " * (3 - len (str (tabCount)))) + Style.RESET_ALL + "| " if prefixWithIndent == True else ""
        if size > 0:
            value += (sizeChar * (size - len (value)))
        value = ("  " * tabCount) + value # Changed to two spaces instead of tab for readability
        if color != Fore.WHITE:
            value = color + value + Style.RESET_ALL
        value = prefix + value    
        if separatorBefore == True:
            value = Pretty.Separator + "\n\n" + value
        if separatorAfter == True:
            value += "\n" + Pretty.Separator + "\n"
        return Pretty.adjust_to_max_length (value, originalValue)
    
    @staticmethod
    def adjust_to_max_length (value:str, originalValue:str) -> str:
        if len (value) <= Pretty.MaxLineLength + 20: # +20 for color information
            return value
        
        separator = " "
        prefixSpaces = " " * value.index (originalValue) #This is not perfect, coloring addes spaces. It is good enough.
        fixedValue = ""
        tokens = value.split (separator)
        lineLength = 0
        for token in tokens:
            if lineLength + len (token) < Pretty.MaxLineLength:
                fixedValue += token + separator
                lineLength += len (token)
            else:
                fixedValue += "\n" + prefixSpaces + token + separator
                lineLength = len (token)
        return fixedValue
            
    @staticmethod
    def assemble_simple (value:str, color=Fore.WHITE) -> str:
        return Pretty.assemble (value=value, separatorBefore=False, separatorAfter=False, color=color, size=0, tabCount=Pretty.Indent)

    @staticmethod 
    def print (value:str, separatorBefore:bool=False, separatorAfter:bool=False, color:str=Fore.WHITE, size:int=0, tabCount:int=0, sizeChar:str=' '):
        print (Pretty.assemble (value=value, separatorBefore=separatorBefore, separatorAfter=separatorAfter, color=color, size=size, tabCount=tabCount, sizeChar=sizeChar))
        return
    
    @staticmethod
    def assemble_output_start_message (printableStartTime:str, functionName:str) -> str:
        message = Pretty.assemble (value=f"{printableStartTime}", prefixWithIndent=True, color=Fore.LIGHTBLACK_EX, tabCount=Pretty.Indent)
        message += Pretty.assemble (value=f" - {functionName} ", prefixWithIndent=False, color=Fore.WHITE, size=80, sizeChar='.') 
        message += " - "
        message += Pretty.assemble (value="Starting", prefixWithIndent=False, color=Fore.LIGHTBLACK_EX)
        return message

    @staticmethod
    def assemble_output_end_message (printableStartTime:str, functionName:str, status:str, printableEndTime:str, executionTime:float) -> str:
        message = Pretty.assemble (value=f"{printableEndTime}", prefixWithIndent=True, color=Fore.LIGHTBLACK_EX, tabCount=Pretty.Indent)
        message += Pretty.assemble (value=f" - {functionName} ", prefixWithIndent=False, color=Fore.WHITE, size=80, sizeChar='.') 
        coloredStatus = Pretty.assemble (value=status, prefixWithIndent=False, color=Fore.GREEN if status == "OK" else Fore.RED) 
        statusMessage = Pretty.assemble (value=f" [{coloredStatus}] ", prefixWithIndent=False, color=Fore.WHITE, size=15) 
        message += statusMessage
        message += " - "
        message += Pretty.assemble (value=f"({printableStartTime})", prefixWithIndent=False, color=Fore.LIGHTBLACK_EX)
        message += " | "
        message += Pretty.assemble (value=f"{round (executionTime, 1)} sec.", prefixWithIndent=False, color=Fore.LIGHTBLACK_EX )
        return message
