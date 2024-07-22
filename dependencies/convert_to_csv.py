
import csv
def read_the_csv(s:str) -> list:
    """
    Takes a string and uses the Python library to convert to a list. 
    The list is further processed to change values to None, float, int, or str, 
    depending if they can be converted. 

    NOTE: In production, I would use more rigorous checking (for example, all fields
    in positiion 0 must be float, etc. But for time, I am using this simple method.
    """
    assert isinstance(s, str)
    for line in csv.reader([s], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL):
        assert len(line) == 150
        final = []
        for i in line:
            i = i.strip()
            if i == '':
                final.append(None)
            elif '%' in i:
                final.append(float(i.replace('%', '').strip()))
            else:
                try:
                    final.append(int(i))
                except ValueError:
                    try:
                        final.append(float(i))
                    except ValueError:
                        final.append(i)
        yield final
