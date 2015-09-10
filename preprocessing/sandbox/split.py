def splitIntoLines(inputfile, outputfile, everynth=40):
    """
    Lazy function to read a file piece by piece.
    Default chunk size: 2kB.
    """
    fout = open(outputfile,"w")
    fin  = open(inputfile,"r")
        
    space_count = 0
    current_line = ""

    data = fin.read()
    for char in data:
        if space_count >= everynth:
            fout.write(current_line+"\n")
            current_line = ""
            space_count = 0        
        if char == " ":
            space_count += 1

        current_line += char    


    fout.close()
    fin.close() 

splitIntoLines("/home/felipe/chess-word2vec/word2vec/text8","out.txt")       