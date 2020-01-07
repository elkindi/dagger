import re


class DQLParser:
    """
    DQL Parser class
    """

    DQL_REGEX = re.compile(
        r"LOOKUP (?P<variable>[\w]+) " + r"WHERE SCOPE ((?P<in>)IN BLOCK " +
        r"(?P<block>[\d]+)|BETWEEN BLOCKS " +
        r"(?P<block1>[\d]+) AND (?P<block2>[\d]+))", re.IGNORECASE)

    @classmethod
    def parse(cls, query):
        match = cls.DQL_REGEX.match(query)
        if match.group('in') is not None:
            start = int(match.group('block'))
            end = start
        else:
            start = int(match.group('block1'))
            end = int(match.group('block2'))
        return {
            'variable': match.group('variable'),
            'start_block': start,
            'end_block': end
        }


def main():
    query_string_1 = "LOOKUP var WHERE SCOPE BETWEEN BLOCKS 2 AND 5"
    query_string_2 = "LOOKUP var WHERE SCOPE IN BLOCK 3"
    query_dict_1 = DQLParser.parse(query_string_1)
    query_dict_2 = DQLParser.parse(query_string_2)
    print(query_dict_1)
    print(query_dict_2)


if __name__ == '__main__':
    main()
