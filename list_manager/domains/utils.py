import re
from enum import IntEnum
from tld import get_tld, get_fld
from .. import log, DataSet

class FileSet(IntEnum):
        domains = 0
        patterns = 1
        invalid = 2
        dup_domains = 3
        dup_patterns = 4

        def __str__(self):
            return f'{self.name}'

class DomainUtils:
    @staticmethod
    def is_pattern(domain) -> bool:
        return re.search(r"[*?^[\]()|$]|(\\.)", domain) is not None

    @staticmethod
    def add(d, major, dup=None):
        if d not in major:
            major.add(d)
        elif dup: 
            if d in dup:
                dup.add(f'{d}_{len(dup)}')
        else:
            major.add(f'{d}_{len(major)}')

    @staticmethod
    def clean_list(lines: list[str]) -> tuple[dict[str, set], dict[str, int]]:
        domains = set()
        patterns = set()
        invalid = set()
        comments = 0
        dup_domains = set()
        dup_patterns = set()

        for line in lines:
            line = line.strip()

            if line.startswith("#") or not line:
                comments += 1
                continue

            parts = line.split("#", 1)[0].split()

            if not len(parts):
                raise (ValueError(f"error: empty after split to parts: {line}"))

            d = parts[1] if len(parts) > 1 else parts[0]

            if "." not in d:
                DomainUtils.add(d, invalid)
                continue

            if DomainUtils.is_pattern(d):
                d = d.replace(".", r"\.")
                d = d.replace("*", r".*")
                d = f"/{d}/"
                DomainUtils.add(d, patterns, dup_patterns)
                continue

            try:
                get_tld(d, fix_protocol=True)
                get_fld(d, fix_protocol=True)
            except Exception:
                DomainUtils.add(d, invalid)
                continue
            
            DomainUtils.add(d, domains, dup_domains)

       
        domain_sets = DataSet({
            FileSet.domains: domains,
            FileSet.patterns: patterns,
            FileSet.invalid: invalid,
            FileSet.dup_domains: dup_domains,
            FileSet.dup_patterns: dup_patterns
        })

        lengths = {k: len(v) for k,v in domain_sets.items()}
        lines_len = len(lines)
        processed = sum(lengths.values()) + comments

        if lines_len != processed:
            log.error("We have a parsing problem")
            raise (ValueError(lengths))
        
        stats =  {
            'lines': lines_len,
            'processed': processed,
            **lengths,
            'comments': comments
        }

        return domain_sets, stats