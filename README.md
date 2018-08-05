# mysql2sqlite

Query MySQL database and mirror relevant tables to a local SQLite database.
This project currently uses a MySQL database used by Postfix/Dovecot mail
servers as the default source database to test against. Future work should
help to generalize the interface so that this project can be used to replicate
any source MySQL database

## Target Audience

Code quality is rough; the target audience is mostly internal users. Please
file an issue for any improvements/suggestions. Pull Requests *are* welcome,
but please base them on obvious improvements (bug or performance issues) or
existing issues already created for the project. Because the current code
is in a state of flux, large PRs may be challenging to implement.

## Related Projects

- This project: <https://github.com/WhyAskWhy/mysql2sqlite>

- [automated-tickets](https://github.com/WhyAskWhy/automated-tickets)
  - These projects share code/design decisions

- [automated-tickets-dev](https://github.com/WhyAskWhy/automated-tickets)

## References / Credits

- <https://workaround.org/ispmail>

- See [this references list](docs/references.md) for other entries not shown here
