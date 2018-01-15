<#--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
-->
<#macro license files component="AsterixDB WebUI" location="${asterixAppLocation!}"
                filePrefix="${asterixAppResourcesPrefix!}"
                licenseName="the following license">
   Portions of the ${component}
<#if location?has_content>
       in: ${location}
</#if>
       located at:
<#if files?is_sequence>
<#list files as file>
<#if file?counter < files?size>
         ${filePrefix}${file},
<#else>
       and
         ${filePrefix}${file}
</#if>
</#list>
<#else>
         ${filePrefix}${files}
</#if>

   are available under ${licenseName}:
---
<@indent spaces=3 unpad=true wrap=true>
<#nested>
</@indent>
---
</#macro>
<@license files=["webui/static/js/jquery.min.js", "webui/static/js/jquery.autosize-min.js", "queryui/js/jquery-1.12.4.min.js"]
          licenseName="an MIT-style license">
   Copyright jQuery Foundation and other contributors, https://jquery.org/

   This software consists of voluntary contributions made by many
   individuals. For exact contribution history, see the revision history
   available at https://github.com/jquery/jquery

   The following license applies to all parts of this software except as
   documented below:

   ====

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
   LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
   OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
   WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

   ====

   All files located in the node_modules and external directories are
   externally maintained libraries used by this software which have their
   own licenses; we recommend you read them, as their terms may differ from
   the terms above.
</@license>
<@license files=["webui/static/js/bootstrap.min.js", "webui/static/css/bootstrap-responsive.min.css", "webui/static/css/bootstrap.min.css", "webui/static/img/glyphicons-halflings-white.png", "webui/static/img/glyphicons-halflings.png"]>
   Copyright 2012 Twitter, Inc.
   http://www.apache.org/licenses/LICENSE-2.0.txt

   Credit for webui/static/img/glyphicons-halflings-white.png,
          and webui/static/img/glyphicons-halflings.png

   GLYPHICONS Halflings font is also released as an extension of a Bootstrap
   (www.getbootstrap.com) for free and it is released under the same license as
   Bootstrap. While you are not required to include attribution on your
   Bootstrap-based projects, I would certainly appreciate any form of support,
   even a nice Tweet is enough. Of course if you want, you can say thank you and
   support me by buying more icons on GLYPHICONS.com.
</@license>
<@license component="AsterixDB runtime" files="org/apache/asterix/hivecompat/io/*"
          licenseName="The Apache License, Version 2.0"
          location="${hivecompatLocation!}" filePrefix="${hivecompatPrefix!}">
Source files in asterix-hivecompat are derived from portions of Apache Hive Query Language v0.13.0 (org.apache.hive:hive-exec).
</@license>
<@license component="AsterixDB WebUI" licenseName="The MIT License"
        files=["webui/static/js/jquery.json-viewer.js","webui/static/css/jquery.json-viewer.css"]>
    Copyright (c) 2014 Alexandre Bodelot

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
</@license>

<@license files="
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/output.component.ts
 asterix-app/src/main/resources/dashboard/static/main.37b7b7cad656490b195a.bundle.js
 asterix-app/src/main/resources/dashboard/static/styles.9f50282210bba5318775.bundle
 asterix-app/src/main/resources/dashboard/static/scripts.da68998bdd77aff4e764.bundle.js
 asterix-app/src/main/resources/dashboard/static/polyfills.32ca5670d6503e090789.bundle.js
 asterix-app/src/main/resources/dashboard/static/inline.66bd6b83f86cf773a001.bundle.js
 asterix-app/src/main/resources/dashboard/static/index.html"
 component=" File Saver portions of the AsterixDB Dashboard"
 licenseName=" the MIT license ">
The MIT License

Copyright © 2016 Eli Grey.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
</@license>
<@license files="
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/datasets-collection/
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/datatypes-collection/
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/dataverses-collection/
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/
 asterix-app/src/main/resources/dashboard/src/app/dashboard/
 asterix-app/src/main/resources/dashboard/src/app/
 asterix-app/src/main/resources/dashboard/static/main.37b7b7cad656490b195a.bundle.js
 asterix-app/src/main/resources/dashboard/static/styles.9f50282210bba5318775.bundle
 asterix-app/src/main/resources/dashboard/static/scripts.da68998bdd77aff4e764.bundle.js
 asterix-app/src/main/resources/dashboard/static/polyfills.32ca5670d6503e090789.bundle.js
 asterix-app/src/main/resources/dashboard/static/inline.66bd6b83f86cf773a001.bundle.js
 asterix-app/src/main/resources/dashboard/static/index.html"
 component="ReactiveX/rxjs"
 licenseName="Apache License 2.0">
                               Apache License
                         Version 2.0, January 2004
                      http://www.apache.org/licenses/

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

 Copyright (c) 2015-2018 Google, Inc., Netflix, Inc., Microsoft Corp. and contributors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
limitations under the License.
</@license>
<@license files="
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/output.component.ts
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/output.component.html
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/output.component.scss
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/metadata.component.ts
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/metadata.component.html
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/metadata.component.scss
 asterix-app/src/main/resources/dashboard/static/main.37b7b7cad656490b195a.bundle.js
 asterix-app/src/main/resources/dashboard/static/styles.9f50282210bba5318775.bundle
 asterix-app/src/main/resources/dashboard/static/scripts.da68998bdd77aff4e764.bundle.js
 asterix-app/src/main/resources/dashboard/static/polyfills.32ca5670d6503e090789.bundle.js
 asterix-app/src/main/resources/dashboard/static/inline.66bd6b83f86cf773a001.bundle.js
 asterix-app/src/main/resources/dashboard/static/index.html"
 component=" PrimeNG components of the AsterixDB dashboard"
 licenseName="The MIT License">


The MIT License (MIT)

Copyright (c) 2016-2017 PrimeTek

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

</@license>
<@license files="
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/datasets-collection/
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/datatypes-collection/
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/dataverses-collection/
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/
 asterix-app/src/main/resources/dashboard/src/app/dashboard/
 asterix-app/src/main/resources/dashboard/src/app/dashboard/
 asterix-app/src/main/resources/dashboard/src/app/
 asterix-app/src/main/resources/dashboard/static/main.37b7b7cad656490b195a.bundle.js
 asterix-app/src/main/resources/dashboard/static/styles.9f50282210bba5318775.bundle
 asterix-app/src/main/resources/dashboard/static/scripts.da68998bdd77aff4e764.bundle.js
 asterix-app/src/main/resources/dashboard/static/polyfills.32ca5670d6503e090789.bundle.js
 asterix-app/src/main/resources/dashboard/static/inline.66bd6b83f86cf773a001.bundle.js
 asterix-app/src/main/resources/dashboard/static/index.html"
 component="NGRX portions of the AsterixDB dashboard"
 licenseName=" The MIT License">


The MIT License (MIT)

Copyright (c) 2017 Brandon Roberts, Mike Ryan, Victor Savkin, Rob Wormald

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

</@license>
<@license files="
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/input.component.ts
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/input.component.html
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/input.component.scss
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/codemirror.component.ts
 asterix-app/src/main/resources/dashboard/src/app/dashboard/query/codemirror.component.scss
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/input-metadata.component.ts
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/input-metadata.component.html
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/input-metadata.component.scss
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/codemirror-metadata.component.ts
 asterix-app/src/main/resources/dashboard/src/app/dashboard/metadata/codemirror-metadata.component.scss
 asterix-app/src/main/resources/dashboard/static/main.37b7b7cad656490b195a.bundle.js
 asterix-app/src/main/resources/dashboard/static/styles.9f50282210bba5318775.bundle
 asterix-app/src/main/resources/dashboard/static/scripts.da68998bdd77aff4e764.bundle.js
 asterix-app/src/main/resources/dashboard/static/polyfills.32ca5670d6503e090789.bundle.js
 asterix-app/src/main/resources/dashboard/static/inline.66bd6b83f86cf773a001.bundle.js
 asterix-app/src/main/resources/dashboard/static/index.html"
 component="Codemirror portions of the AsterixDB dashboard"
 licenseName="The MIT License">


Copyright (C) 2017 by Marijn Haverbeke <marijnh@gmail.com> and others

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

</@license>
<@license files="
 asterix-app/src/main/resources/dashboard/src/
 asterix-app/src/main/resources/dashboard/static/main.37b7b7cad656490b195a.bundle.js
 asterix-app/src/main/resources/dashboard/static/styles.9f50282210bba5318775.bundle
 asterix-app/src/main/resources/dashboard/static/scripts.da68998bdd77aff4e764.bundle.js
 asterix-app/src/main/resources/dashboard/static/polyfills.32ca5670d6503e090789.bundle.js
 asterix-app/src/main/resources/dashboard/static/inline.66bd6b83f86cf773a001.bundle.js
 asterix-app/src/main/resources/dashboard/static/index.html"
 component="AngularJS portions of the AsterixDB dashboard"
 licenseName="The MIT License">


The MIT License

Copyright (c) 2014-2017 Google, Inc. http://angular.io

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

</@license>
<@license files="
 asterix-app/src/main/resources/dashboard/src/app/dashboard/
 asterix-app/src/main/resources/dashboard/src/app/material.module.ts
 asterix-app/src/main/resources/dashboard/src/main.ts
 asterix-app/src/main/resources/dashboard/static/main.37b7b7cad656490b195a.bundle.js
 asterix-app/src/main/resources/dashboard/static/styles.9f50282210bba5318775.bundle
 asterix-app/src/main/resources/dashboard/static/scripts.da68998bdd77aff4e764.bundle.js
 asterix-app/src/main/resources/dashboard/static/polyfills.32ca5670d6503e090789.bundle.js
 asterix-app/src/main/resources/dashboard/static/inline.66bd6b83f86cf773a001.bundle.js
 asterix-app/src/main/resources/dashboard/static/index.html"
 component="Angular Material and HammerJS portions of the AsterixDB dashboard"
 licenseName="The MIT License">

Copyright (c) 2017 Google LLC.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

</@license>
<@license files="
 asterix-app/src/main/resources/dashboard/src/
 asterix-app/src/main/resources/dashboard/static/main.37b7b7cad656490b195a.bundle.js
 asterix-app/src/main/resources/dashboard/static/styles.9f50282210bba5318775.bundle
 asterix-app/src/main/resources/dashboard/static/scripts.da68998bdd77aff4e764.bundle.js
 asterix-app/src/main/resources/dashboard/static/polyfills.32ca5670d6503e090789.bundle.js
 asterix-app/src/main/resources/dashboard/static/inline.66bd6b83f86cf773a001.bundle.js
 asterix-app/src/main/resources/dashboard/static/index.html"
 component="CoreJS portions of the AsterixDB dashboard"
 licenseName="The MIT License">


Copyright (c) 2014-2017 Denis Pushkarev

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

</@license>
<@license files="
 asterix-app/src/main/resources/dashboard/src/index.html
 asterix-app/src/main/resources/dashboard/static/roboto-v15-latin-regular.3d3a53586bd78d1069ae.svg
 asterix-app/src/main/resources/dashboard/static/roboto-v15-latin-regular.7e367be02cd17a96d513.woff2
 asterix-app/src/main/resources/dashboard/static/roboto-v15-latin-regular.9f916e330c478bbfa2a0.eot
 asterix-app/src/main/resources/dashboard/static/roboto-v15-latin-regular.16e1d930cf13fb7a9563.woff
 asterix-app/src/main/resources/dashboard/static/roboto-v15-latin-regular.38861cba61c66739c145.ttf
 asterix-app/src/main/resources/dashboard/static/index.html
"
 component="Roboto Font portions of the AsterixDB dashboard"
 licenseName="Apache License 2.0">
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

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

   Copyright [yyyy] [name of copyright owner]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
limitations under the License.</@license>
<@license files="
 asterix-app/src/main/resources/dashboard/src/index.html
 asterix-app/src/main/resources/dashboard/static/index.html"
 component="Font Awesome portions of the AsterixDB dashboard"
 licenseName="The MIT License">

    http://fontawesome.io/license/ Applies to all CSS and LESS files in the
    following directories if exist: font-awesome/css/, font-awesome/less/, and
    font-awesome/scss/.  and downloaded from CDN network services and loaded in
    index.html License: MIT License URL:
    http://opensource.org/licenses/mit-license.html


</@license>
<@license files="asterix-app/src/main/resources/dashboard/"
 component=" webpack portions of the AsterixDB dashboard"
 licenseName=" The MIT License">
Copyright JS Foundation and other contributors

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

</@license>

<@license files="
 asterix-app/src/main/resources/dashboard/src/
 asterix-app/src/main/resources/dashboard/static/main.37b7b7cad656490b195a.bundle.js
 asterix-app/src/main/resources/dashboard/static/styles.9f50282210bba5318775.bundle
 asterix-app/src/main/resources/dashboard/static/scripts.da68998bdd77aff4e764.bundle.js
 asterix-app/src/main/resources/dashboard/static/polyfills.32ca5670d6503e090789.bundle.js
 asterix-app/src/main/resources/dashboard/static/inline.66bd6b83f86cf773a001.bundle.js
 asterix-app/src/main/resources/dashboard/static/index.html"
 component="zonejs portions of the AsterixDB dashboard"
 licenseName=" The MIT License">

Copyright (c) 2016 Google, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
</@license>
