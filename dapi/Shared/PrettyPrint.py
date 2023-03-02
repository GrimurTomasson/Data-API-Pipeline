from colorama import Fore, Style

class Pretty:
    Separator = "-" * 120

    @staticmethod
    def assemble (value, separatorBefore=False, separatorAfter=False, color=Fore.WHITE, size=0, tabCount=0):
        if size > 0:
            value += (" " * (size - len (value)))
        value = ("\t" * tabCount) + value
        if color != Fore.WHITE:
            value = color + value + Style.RESET_ALL
        if separatorBefore == True:
            value = Pretty.Separator + "\n\n" + value
        if separatorAfter == True:
            value += "\n" + Pretty.Separator + "\n"
        return value

    @staticmethod 
    def print (line, separatorBefore=False, separatorAfter=False, color=Fore.WHITE, size=0, tabCount=0):
        print (Pretty.assemble (line, separatorBefore, separatorAfter, color, tabCount))
        return