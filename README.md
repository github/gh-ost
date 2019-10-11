# gh-ost

#### GitHub's online schema migration for MySQL <img src="doc/images/gh-ost-logo-light-160.png" align="right">

 `gh-ost` is a triggerless online schema migration solution for MySQL. It is testable and provides pausability, dynamic control/reconfiguration, auditing, and many operational perks.

`gh-ost` produces a light workload on the master throughout the migration, decoupled from the existing workload on the migrated table.

It has been designed based on years of experience with existing solutions, and changes the paradigm of table migrations.



## How?

All existing online-schema-change tools operate in similar manner: they create a _ghost_ table in the likeness of your original table, migrate that table while empty, slowly and incrementally copy data from your original table to the _ghost_ table, meanwhile propagating ongoing changes (any `INSERT`, `DELETE`, `UPDATE` applied to your table) to the _ghost_ table. Finally, at the right time, they replace your original table with the _ghost_ table.

`gh-ost` uses the same pattern. However it differs from all existing tools by not using triggers. We have recognized the triggers to be the source of [many limitations and risks](doc/why-triggerless.md).

Instead, `gh-ost` [uses the binary log stream](doc/triggerless-design.md) to capture table changes, and asynchronously applies them onto the _ghost_ table. `gh-ost` takes upon itself some tasks that other tools leave for the database to perform. As result, `gh-ost` has greater control over the migration process; can truly suspend it; can truly decouple the migration's write load from the master's workload.

In addition, it offers many [operational perks](doc/perks.md) that make it safer, trustworthy and fun to use.

![gh-ost general flow](doc/images/gh-ost-general-flow.png)

## Highlights

- Build your trust in `gh-ost` by testing it on replicas. `gh-ost` will issue same flow as it would have on the master, to migrate a table on a replica, without actually replacing the original table, leaving the replica with two tables you can then compare and satisfy yourself that the tool operates correctly. This is how we continuously test `gh-ost` in production.
- True pause: when `gh-ost` [throttles](doc/throttle.md), it truly ceases writes on master: no row copies and no ongoing events processing. By throttling, you return your master to its original workload
- Dynamic control: you can [interactively](doc/interactive-commands.md) reconfigure `gh-ost`, even as migration still runs. You may forcibly initiate throttling.
- Auditing: you may query `gh-ost` for status. `gh-ost` listens on unix socket or TCP.
- Control over cut-over phase: `gh-ost` can be instructed to postpone what is probably the most critical step: the swap of tables, until such time that you're comfortably available. No need to worry about ETA being outside office hours.
- External [hooks](doc/hooks.md) can couple `gh-ost` with your particular environment.

Please refer to the [docs](doc) for more information. No, really, read the [docs](doc).

## Usage

The [cheatsheet](doc/cheatsheet.md) has it all. You may be interested in invoking `gh-ost` in various modes:

- a _noop_ migration (merely testing that the migration is valid and good to go)
- a real migration, utilizing a replica (the migration runs on the master; `gh-ost` figures out identities of servers involved. Required mode if your master uses Statement Based Replication)
- a real migration, run directly on the master (but `gh-ost` prefers the former)
- a real migration on a replica (master untouched)
- a test migration on a replica, the way for you to build trust with `gh-ost`'s operation.

Our tips:

- [Testing above all](doc/testing-on-replica.md), try out `--test-on-replica` first few times. Better yet, make it continuous. We have multiple replicas where we iterate our entire fleet of production tables, migrating them one by one, checksumming the results, verifying migration is good.
- For each master migration, first issue a _noop_
- Then issue the real thing via `--execute`.

More tips:

- Use `--exact-rowcount` for accurate progress indication
- Use `--postpone-cut-over-flag-file` to gain control over cut-over timing
- Get familiar with the [interactive commands](doc/interactive-commands.md)

Also see:

- [requirements and limitations](doc/requirements-and-limitations.md)
- [what if?](doc/what-if.md)
- [the fine print](doc/the-fine-print.md)

## What's in a name?

Originally this was named `gh-osc`: GitHub Online Schema Change, in the likes of [Facebook online schema change](https://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932/) and [pt-online-schema-change](https://www.percona.com/doc/percona-toolkit/2.2/pt-online-schema-change.html).

But then a rare genetic mutation happened, and the `c` transformed into `t`. And that sent us down the path of trying to figure out a new acronym. `gh-ost` (pronounce: _Ghost_), stands for GitHub's Online Schema Transmogrifier/Translator/Transformer/Transfigurator

## License

`gh-ost` is licensed under the [MIT license](https://github.com/github/gh-ost/blob/master/LICENSE)

`gh-ost` uses 3rd party libraries, each with their own license. These are found [here](https://github.com/github/gh-ost/tree/master/vendor).

## Community

`gh-ost` is released at a stable state, but with mileage to go. We are [open to pull requests](https://github.com/github/gh-ost/blob/master/.github/CONTRIBUTING.md). Please first discuss your intentions via [Issues](https://github.com/github/gh-ost/issues).

We develop `gh-ost` at GitHub and for the community. We may have different priorities than others. From time to time we may suggest a contribution that is not on our immediate roadmap but which may appeal to others.

## Download/binaries/source

`gh-ost` is now GA and stable.

`gh-ost` is available in binary format for Linux and Mac OS/X

[Download latest release here](https://github.com/github/gh-ost/releases/latest)

`gh-ost` is a Go project; it is built with Go 1.5 with "experimental vendor". Soon to migrate to Go 1.6. See and use [build file](https://github.com/github/gh-ost/blob/master/build.sh) for compiling it on your own.

Generally speaking, `master` branch is stable, but only [releases](https://github.com/github/gh-ost/releases) are to be used in production.

## Authors

`gh-ost` is designed, authored, reviewed and tested by the database infrastructure team at GitHub:
- [@jonahberquist](https://github.com/jonahberquist)
- [@ggunson](https://github.com/ggunson)
- [@tomkrouper](https://github.com/tomkrouper)
- [@shlomi-noach](https://github.com/shlomi-noach)



Apache License
                           Version 2.0, January 2004
                        https://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

   1. Definitions.

      "License" shall mean the terms and conditions for use, reproduction,
      and distribution as defined by Sections 1 through 9 of this document.

      "Licensor" shall mean the copyright owner or entity authorized by
      the copyright owner that is granting the License.

      "Legal Entity" shall mean the union of the acting entity and all
      other entities that control, are controlled by, or are under common
      control with that entity. For the purposes of this definition,
      "control" means (i) the power, direct or indirect, to cause the
      direction or management of such entity, whether by contract or
      otherwise, or (ii) ownership of fifty percent (50%) or more of the
      outstanding shares, or (iii) beneficial ownership of such entity.

      "You" (or "Your") shall mean an individual or Legal Entity
      exercising permissions granted by this License.

      "Source" form shall mean the preferred form for making modifications,
      including but not limited to software source code, documentation
      source, and configuration files.

      "Object" form shall mean any form resulting from mechanical
      transformation or translation of a Source form, including but
      not limited to compiled object code, generated documentation,
      and conversions to other media types.

      "Work" shall mean the work of authorship, whether in Source or
      Object form, made available under the License, as indicated by a
      copyright notice that is included in or attached to the work
      (an example is provided in the Appendix below).

      "Derivative Works" shall mean any work, whether in Source or Object
      form, that is based on (or derived from) the Work and for which the
      editorial revisions, annotations, elaborations, or other modifications
      represent, as a whole, an original work of authorship. For the purposes
      of this License, Derivative Works shall not include works that remain
      separable from, or merely link (or bind by name) to the interfaces of,
      the Work and Derivative Works thereof.

      "Contribution" shall mean any work of authorship, including
      the original version of the Work and any modifications or additions
      to that Work or Derivative Works thereof, that is intentionally
      submitted to Licensor for inclusion in the Work by the copyright owner
      or by an individual or Legal Entity authorized to submit on behalf of
      the copyright owner. For the purposes of this definition, "submitted"
      means any form of electronic, verbal, or written communication sent
      to the Licensor or its representatives, including but not limited to
      communication on electronic mailing lists, source code control systems,
      and issue tracking systems that are managed by, or on behalf of, the
      Licensor for the purpose of discussing and improving the Work, but
      excluding communication that is conspicuously marked or otherwise
      designated in writing by the copyright owner as "Not a Contribution."

      "Contributor" shall mean Licensor and any individual or Legal Entity
      on behalf of whom a Contribution has been received by Licensor and
      subsequently incorporated within the Work.

   2. Grant of Copyright License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      copyright license to reproduce, prepare Derivative Works of,
      publicly display, publicly perform, sublicense, and distribute the
      Work and such Derivative Works in Source or Object form.

   3. Grant of Patent License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      (except as stated in this section) patent license to make, have made,
      use, offer to sell, sell, import, and otherwise transfer the Work,
      where such license applies only to those patent claims licensable
      by such Contributor that are necessarily infringed by their
      Contribution(s) alone or by combination of their Contribution(s)
      with the Work to which such Contribution(s) was submitted. If You
      institute patent litigation against any entity (including a
      cross-claim or counterclaim in a lawsuit) alleging that the Work
      or a Contribution incorporated within the Work constitutes direct
      or contributory patent infringement, then any patent licenses
      granted to You under this License for that Work shall terminate
      as of the date such litigation is filed.

   4. Redistribution. You may reproduce and distribute copies of the
      Work or Derivative Works thereof in any medium, with or without
      modifications, and in Source or Object form, provided that You
      meet the following conditions:

      (a) You must give any other recipients of the Work or
          Derivative Works a copy of this License; and

      (b) You must cause any modified files to carry prominent notices
          stating that You changed the files; and

      (c) You must retain, in the Source form of any Derivative Works
          that You distribute, all copyright, patent, trademark, and
          attribution notices from the Source form of the Work,
          excluding those notices that do not pertain to any part of
          the Derivative Works; and

      (d) If the Work includes a "NOTICE" text file as part of its
          distribution, then any Derivative Works that You distribute must
          include a readable copy of the attribution notices contained
          within such NOTICE file, excluding those notices that do not
          pertain to any part of the Derivative Works, in at least one
          of the following places: within a NOTICE text file distributed
          as part of the Derivative Works; within the Source form or
          documentation, if provided along with the Derivative Works; or,
          within a display generated by the Derivative Works, if and
          wherever such third-party notices normally appear. The contents
          of the NOTICE file are for informational purposes only and
          do not modify the License. You may add Your own attribution
          notices within Derivative Works that You distribute, alongside
          or as an addendum to the NOTICE text from the Work, provided
          that such additional attribution notices cannot be construed
          as modifying the License.

      You may add Your own copyright statement to Your modifications and
      may provide additional or different license terms and conditions
      for use, reproduction, or distribution of Your modifications, or
      for any such Derivative Works as a whole, provided Your use,
      reproduction, and distribution of the Work otherwise complies with
      the conditions stated in this License.

   5. Submission of Contributions. Unless You explicitly state otherwise,
      any Contribution intentionally submitted for inclusion in the Work
      by You to the Licensor shall be under the terms and conditions of
      this License, without any additional terms or conditions.
      Notwithstanding the above, nothing herein shall supersede or modify
      the terms of any separate license agreement you may have executed
      with Licensor regarding such Contributions.

   6. Trademarks. This License does not grant permission to use the trade
      names, trademarks, service marks, or product names of the Licensor,
      except as required for reasonable and customary use in describing the
      origin of the Work and reproducing the content of the NOTICE file.

   7. Disclaimer of Warranty. Unless required by applicable law or
      agreed to in writing, Licensor provides the Work (and each
      Contributor provides its Contributions) on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
      implied, including, without limitation, any warranties or conditions
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
      PARTICULAR PURPOSE. You are solely responsible for determining the
      appropriateness of using or redistributing the Work and assume any
      risks associated with Your exercise of permissions under this License.

   8. Limitation of Liability. In no event and under no legal theory,
      whether in tort (including negligence), contract, or otherwise,
      unless required by applicable law (such as deliberate and grossly
      negligent acts) or agreed to in writing, shall any Contributor be
      liable to You for damages, including any direct, indirect, special,
      incidental, or consequential damages of any character arising as a
      result of this License or out of the use or inability to use the
      Work (including but not limited to damages for loss of goodwill,
      work stoppage, computer failure or malfunction, or any and all
      other commercial damages or losses), even if such Contributor
      has been advised of the possibility of such damages.

   9. Accepting Warranty or Additional Liability. While redistributing
      the Work or Derivative Works thereof, You may choose to offer,
      and charge a fee for, acceptance of support, warranty, indemnity,
      or other liability obligations and/or rights consistent with this
      License. However, in accepting such obligations, You may act only
      on Your own behalf and on Your sole responsibility, not on behalf
      of any other Contributor, and only if You agree to indemnify,
      defend, and hold each Contributor harmless for any liability
      incurred by, or claims asserted against, such Contributor by reason
      of your accepting any such warranty or additional liability.

   END OF TERMS AND CONDITIONS

   APPENDIX: How to apply the Apache License to your work.

      To apply the Apache License to your work, attach the following
      boilerplate notice, with the fields enclosed by brackets "[]"
      replaced with your own identifying information. (Don't include
      the brackets!)  The text should be enclosed in the appropriate
      comment syntax for the file format. We also recommend that a
      file or class name and description of purpose be included on the
      same "printed page" as the copyright notice for easier
      identification within third-party archives.

   Copyright 2019 Rolando Gopez Lacuata

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


