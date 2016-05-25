# gh-ost

#### GitHub's online schema migration for MySQL

`gh-ost` allows for online schema migrations in MySQL which are:
- Triggerless
- Testable
- Pausable
- Operations-friendly

## How?

WORK IN PROGRESS

Please meanwhile refer to the [docs](doc) for more information.

## What's in a name?

Originally this was named `gh-osc`: GitHub Online Schema Change, in the likes of [Facebook online schema change](https://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932/) and [pt-online-schema-change](https://www.percona.com/doc/percona-toolkit/2.2/pt-online-schema-change.html).

But then a rare genetic mutation happened, and the `s` transformed into `t`. And that sent us down the path of trying to figure out a new acronym. Right now, `gh-ost` (pronounce: _Ghost_), stands for:
- GitHub Online Schema Translator/Transformer/Transfigurator

## Authors

`gh-ost` is designed, authored, reviewed and tested by the database infrastructure team at GitHub:
- [@jonahberquist](https://github.com/jonahberquist)
- [@ggunson](https://github.com/ggunson)
- [@tomkrouper](https://github.com/tomkrouper)
- [@shlomi-noach](https://github.com/shlomi-noach)
