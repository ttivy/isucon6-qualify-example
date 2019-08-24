import ahocorasick

class Replacer(object):
    def __init__(self, replacements={}):
        self.automaton = ahocorasick.Automaton()
        for src, dst in replacements.items():
            self.automaton.add_word(src, (src, dst))

    def replace(self, content):
        # finalize automaton
        self.automaton.make_automaton()

        # do nothing if not finalized
        if self.automaton.kind != ahocorasick.AHOCORASICK:
            return content

        def _find_all():
            for end, (src, dst) in self.automaton.iter(content):
                start = end - len(src) + 1
                yield start, end, dst

        def _generator():
            cur = 0
            for start, end, dst in sorted(_find_all(), key=lambda x: (x[0], -x[1])):
                if start > cur:
                    yield content[cur:start]
                if start < cur or end < cur:
                    continue
                yield dst
                cur = end + 1
            yield content[cur:]

        return ''.join(_generator())

# Test
if __name__ == '__main__':
    rep = Replacer({
        'abe': '<ABE>',
        'est': '<EST>',
        'best': '<BEST>',
        'test': '<TEST>'
    })
    result = rep.replace('abestestbes')
    print(result)
    try:
        assert result == '<ABE>s<TEST>bes'
    except:
        raise
