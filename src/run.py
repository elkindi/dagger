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


def save(value: object, name: str, lineno: int, db: DbInterface):
    obj = DbObject(type(value), datetime.now(ltz), lineno, name, value)
    try:
        db.save(obj)
    except Exception as e:
        log.append((obj, False, e))
    else:
        log.append((obj, True))


def main(name, blocks=[], modifier_attr_fcts=[]):

    source = open(name, 'r').read()
    tree = ast.parse(source)

    logger = Logger(save, 'val', 'name', 'lineno', 'db')
    logger.add_blocks(blocks)
    logger.add_modifier_attr_fcts(modifier_attr_fcts)

    print("Lines checked:")
    print(logger.get_blocks())
    print("Modifier functions logged:")
    print(list(logger.get_modifier_attr_fcts()))

    # print("Tree:")
    # astpretty.pprint(tree)

    new_tree = logger.visit(tree)
    code = compile(new_tree, name, 'exec')

    print("Execution:")
    with DbResource() as db:
        exec(code, {'save': save, 'db': db})

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

    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='python file to run')
    parser.add_argument('-b',
                        '--blocks',
                        dest='blocks',
                        default=[],
                        type=block_list,
                        nargs='*',
                        help='Pipeline blocks')
    parser.add_argument('-maf',
                        '--modifier_attr_fcts',
                        default=[],
                        dest='modifier_attr_fcts',
                        type=str,
                        nargs='*',
                        help='Modifier attribute functions')
    args = parser.parse_args()
    print(args)
    main(args.filename,
         blocks=args.blocks,
         modifier_attr_fcts=args.modifier_attr_fcts)