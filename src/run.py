import ast
import argparse
import astpretty
from block import Block
from logger import Logger
from datetime import datetime
from dbObject import DbObject
from dbInterface import DbResource, DbInterface

log = []
ltz = datetime.utcnow().astimezone().tzinfo


# Saves the variable in the database and adds an entry in the log
def log_variable(value: object,
                 name: str = None,
                 lineno: int = None,
                 db: DbInterface = None):
    if name is None:
        return
    obj = DbObject(type(value), datetime.now(ltz), lineno, name, value)
    try:
        db.save(obj)
    except Exception as e:
        log.append((obj, False, e))
    else:
        log.append((obj, True))


# Runs a python script file and saves the variables defined in the given blocks
# The modifier attribute functions are the functions that modify a given variable without an assignment. ex: append, pop
def main(name, blocks=[], modifier_attr_fcts=[], delta_logging=True):

    # Read the script file and parse the code into an ast
    source = open(name, 'r').read()
    tree = ast.parse(source)

    # Create a Logger wih a given log function and specify the blocks and mafs
    logger = Logger(log_variable, 'val', 'name', 'lineno', 'db')
    logger.add_blocks(blocks)
    logger.add_modifier_attr_fcts(modifier_attr_fcts)

    print("Lines checked:")
    print(logger.get_blocks())
    print("Modifier functions logged:")
    print(list(logger.get_modifier_attr_fcts()))

    # print("Tree:")
    # astpretty.pprint(tree)

    # Visit the ast of the source code and add the needed logging functions
    new_tree = logger.visit(tree)
    code = compile(new_tree, name, 'exec')

    # print("New Tree:")
    # astpretty.pprint(new_tree)

    # Execute the new code
    db_resource = DbResource(delta_logging)
    with db_resource as db:
        print("Execution:")
        exec(code, {'log': log, 'log_variable': log_variable, 'db': db})

    # Print the log after execution
    print("Log:")
    for log_item in log:
        if log_item[1]:
            print("{}: Saved {}".format(log_item[0].time.strftime('%H:%M:%S'),
                                        log_item[0]))
        else:
            print("{}: Not Saved {}".format(
                log_item[0].time.strftime('%H:%M:%S'), log_item[0]))
            print("Message: {}".format(log_item[2]))


if __name__ == '__main__':

    # Creates a block from a tuple of line numbers written as: (start,end)
    def block_list(s):
        try:
            block = tuple(map(int, s.strip('()').split(',')))
            if len(block) == 2 and block[0] < block[1]:
                return Block(block[0], block[1])
            else:
                raise argparse.ArgumentTypeError(
                    "Blocks must be defined as '(start,end)', with end > start"
                )
        except:
            raise argparse.ArgumentTypeError(
                "Blocks must be defined as '(start,end)', with end > start")

    # Parse input string into a boolean value
    def str2bool(v):
        if isinstance(v, bool):
           return v
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')

    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='python file to run')  # Get the filename
    parser.add_argument(
        '-b',
        '--blocks',
        dest='b',
        default=[],
        type=block_list,
        nargs='*',
        help='Pipeline blocks written as (start,end)'
    )  # Get the list of blocks to inspect, if no blocks are given, all lines are checked
    parser.add_argument('-maf',
                        '--modifier_attr_fcts',
                        default=[],
                        dest='maf',
                        type=str,
                        nargs='*',
                        help='Modifier attribute functions (ex: append)'
                        )  # Get the list of modifier attribute functions
    parser.add_argument('-dl',
                        '--delta_logging',
                        const=True,
                        default=False,
                        dest='dl',
                        type=str2bool,
                        nargs='?',
                        help='Save dataframes using delta logging or not'
                        )  # Save values with delta logging or not
    args = parser.parse_args()
    main(args.filename, blocks=args.b, modifier_attr_fcts=args.maf, delta_logging=args.dl)