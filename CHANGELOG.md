# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

## 1.25.0 (2025-09-02)


### 🛠 Refacto

* **admin:** Rename components ([876bce4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/876bce4de9ab58125d075e2c019ae5e245e702f5))
* **api:** Simplify ([968899a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/968899a0fd1afacc9f59d3c1f7b8e2f953ed4348))
* **converters-v2:** Move to ts ([9340b38](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9340b38354f04c09a7cfa2f7c093b880439f9c02))
* **export-v2:** Move to ts ([69e86a5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/69e86a5ef74d862e0b7d6e8d2bb1d64e66a5da3c))
* **import-v2:** Map file ids to database ids ([e0b9383](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e0b9383c344a3328383bf8c562513041f703baa0))
* **import-v2:** Move to ts ([f492927](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f492927d2f83c08466e6f38899f20bfdf2adf051))
* **ImportEditor:** Translate component ([52b0303](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/52b0303e39057836cecf5ccd7b9615a3f1906406))
* **models:** Move to ts ([c9dadef](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c9dadef6ac4d511f3e8927b99d340ec0cecedcfb))
* **test:** Rename test files ([5e9f695](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5e9f69566028e47de09f90d2a2d88cc4c71dcc49))
* **tests:** Move tests to specs folder ([6f0f85c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6f0f85c73999e38bde82765b418b32789cf27876))


### 🐞 Bug Fixes

* **admin:** use AnErrorOccurred instead of missing NotFound ([ac993e7](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ac993e7b911513e48660694a6a8711a92bd46b5a)), closes [#129](https://github.com/batemir/strapi-plugin-import-export-entries/issues/129)
* application/vnd.ms-excel not supported. ([2ff0804](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/2ff0804b33379ba29c3cc2a8c8b9e68882d33051))
* back to node-fetch v2 ([aa018cc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/aa018cc5028e8f36d10adcae75193a7e068557c5))
* **convert-data:** Export all entity attributes ([edd8a18](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/edd8a18a65ef2dec5fecd607867843e202c756c4))
* **convert-data:** Export relations as id ([bb0e540](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/bb0e540755bbc24fab5e9dd2332cf709fddb7625))
* **convert-data:** Stringify objects when export in csv ([b2106bc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b2106bcc01b9c072b907b5cb233728416f86575d))
* Define const ([1cd1585](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1cd1585c269a73a1ec04366b1d66303bb6629901))
* **doc:** Make relations import clearer ([cf88780](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/cf887805be3f8194f8c86763e0e14607c0ecb7cb))
* **doc:** Set anchor to section ([435b5f9](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/435b5f9b8b3b34166d35d755ac64184ac50a01ff))
* **Editor:** Load monaco editor locally ([6bf1e15](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6bf1e1594fb58fb068bff04a6bfc64288d88da77))
* **eslint:** Add jest config ([8f2d37c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8f2d37c63be61c2443b8d834a56e5359327b783b))
* **export-v2:** Export components for simple single and collection types ([c954791](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c9547913b4699da87fac2f5d16345a59669600f7))
* **export-v2:** Export selected slugs ([a9482bd](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a9482bd24286a190df4d73f9a5f0416eb6c027dd))
* **export-v2:** Export single type localized ([09dc3cc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/09dc3ccaac27921c6c5f2c9fc822886784c3eaf3))
* **export-v2:** Filter out ids using attribute slug ([4e6e4e7](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4e6e4e77c3aa63b223a67902ad99c3baacf99835))
* **export-v2:** Remove duplicated components in dynamic zones ([eb7ce26](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/eb7ce26c8fd10b2d418ea1e117248db479656f71))
* **export:** Adapt sort query to entity service ([94c77eb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/94c77eb02aeff92d0c60d9bc8848944ac72bfee3))
* **export:** Apply filters fix, replaceAll replaced with replace method for CSV conversion ([696254d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/696254d5ce4f1432e31ab0bed06d76cb9070b46a))
* **export:** Export dynamic zone ([635dc72](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/635dc724cad3652ecb8db6fd22faead815464d11))
* **export:** Handle export relations as ids for one to many relations ([44f25b5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/44f25b5ccd46ae60c28e32eb480b38411291379b))
* **export:** Handle null when export relations as id ([b01d246](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b01d2460f10b4768389736b2ff3a722b7c4ea69c))
* **export:** Sub populate seo component ([ab5d227](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ab5d227bffa0592bce855ebdc7ce2d655edaf55e))
* **file:** Set empty caption and alternative text ([b9ad57c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b9ad57c4dfc645ac6707e7e9a9961080eb3d6aa9))
* **file:** Set right file name ([113fed1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/113fed10f9df8822fe94ddd6979ea3eb2b2a60d4))
* **gitignore:** Ignore only folders at project root ([8a172ab](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8a172ab0540e379606b8a89495534b7b4532d1b3))
* Have component export/import compatible with entity service ([5006837](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5006837327e63fd48322a177a63ee7aab5d8f1bb))
* **HomePage:** Display alerts ([8b55716](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8b557162a1f183e15b9d3fc53e346ef94986dbba))
* **import-data:** Check content permissions when import whole database ([c5c8727](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c5c8727285f12f8b086300f1807038611ee53474))
* **import-v2:** Fail when missing required field ([fda6ca2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fda6ca2c856a29cb88f1d93ba08925b3bd71b989))
* **import-v2:** Ignore slugs with no associated schema ([c6efb54](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c6efb5461b08265173f68bc312b13dc5b9d752b9))
* **import-v2:** Import collection types localized ([4a2c913](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4a2c9131a27e1c3f3e0175cc3aa25cf7751014f3))
* **import-v2:** Import components and dynamic zones for not localized content types ([378a7e3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/378a7e37627c8e1451eadac28ae0b5c431308442))
* **import-v2:** Import media files once ([f0a4518](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f0a4518421a11968f0c6903d4f4088c680ba1e9b))
* **import-v2:** Import single type ([35b54c5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/35b54c5d4ec730f9159059589a0ba1c522b36565))
* **import-v2:** Import single type with specified id ([1e66da1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1e66da1a4500f20acd27d17b4ad2c66d6d806cbf))
* **import-v2:** Set components for simple single and collection types ([366461f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/366461f6c548cc808d2a0fd4823c333e58d08d99))
* **import-v2:** Sync primary key sequence for postgres database ([5220b64](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5220b6456169be6357c2e092cec13c0e4f8c310a))
* **import:** change deprecated request by node-fetch module ([f1e63bb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f1e63bb27095e2922e7eae9395e1f2e79f6b4056))
* **ImportEditor:** Make import options more obvious ([e13fa70](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e13fa70b72d50fa451c4faa54bc127cf89e316c9))
* **import:** fallback to any when media field is missing allowedTypes ([0cce657](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0cce657d322ac39d737c74e0b7fc1897bab79b49))
* **import:** Handle import of one to many relations ([af6daab](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/af6daab950e71a3959d263c7d59981b6b92c6007))
* **import:** Import components and dynamic zones sequentially ([6d5f2f1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6d5f2f1f8ae13b5d067fb843d32b443c59bad3af))
* **import:** Import entries with dependencies ([413362b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/413362bc141ca096b28e36601ab787e7961f3eaf))
* **import:** Import media ([c8f0aad](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c8f0aad9d82c5951dd87739286ef49c435896a04))
* **import:** Import/Set media by id, url or object ([c85bfe2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c85bfe2c6a7999f540ab6c88021b477007b4bab2))
* **ImportModal:** Handle error payload too large ([2b0159f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/2b0159f67d9d3ed4c178f2c1081c78a3339bde97))
* **import:** Support import of dynamic zones ([1ec2b04](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1ec2b046aec5176f5741b9087bbe27f844b7b232))
* **import:** Support import of relations defined in any order ([9a481a5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9a481a58738c9f4c1554d8f4b1d8d2f561222e37))
* **import:** Use current admin user id for create/update fields ([fc0408a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fc0408ad3c772e44f7cad29bfd490a46b27a07b2))
* **package:** Only clean build folders on publish ([97e84cb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/97e84cb734ed98908e8af162a686c95d3576f0bb))
* **README:** Remove webpack config section ([41a32e6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/41a32e61038813f63f932caa37d773d8e70f908a))
* **README:** Update discord invite link ([4b1be2c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4b1be2c58a49b8a13610969ceb0218b4f983b63f))
* **test:** Await database cleanup ([f47fb89](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f47fb891cea3edaf9e7713c1fcc69dcd95a2365f))
* **tests:** Comment date tests temporarily for CI ([6f5128c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6f5128c76f48592d8c974eab31e3d8cb589503fe))
* **tests:** Reset auto increment sequence ([6ca9fbb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6ca9fbb425767b090563ea798a2139a527bf7c5e))
* **types:** Fix typing ([5c86e4f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5c86e4f5bc42a68c1e0d852c4092930729c4db84))
* Update plugin name ([20a4b6d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/20a4b6dd2dbefefea2be200ada3fb7160c9ae785))
* Update request urls ([f27b3c9](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f27b3c96769daa6b73ce0339017783e7335ad709))
* **useSlug:** Extract slug on collection type entry page ([8022306](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8022306c106e2dc9a09916721bb488b68f8c8d49))
* **utils:** Remove non existent exports ([6dfef02](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6dfef02e78296dc484523028ef8b81c70c8be63f))


### 🧪 Tests

* **collection-type:** Create localized collection type ([efa58f5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/efa58f52f66b883d447bc7725a11b6cfa95eea83))
* **config:** Define global strapi for ts ([de75c22](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/de75c22fe876eaae97e24b2d1063c3d2ccbc5b9f))
* **export-v2:** Should export collection type localized ([454e2d3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/454e2d345b0cadaca40e69289a8db4e762ccc6ab))
* **export-v2:** Should export components for simple single and collection types ([c1e4528](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c1e452841f68d3441caf560259e7a4b07ab53614))
* **export-v2:** Should export whole database ([7cab210](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/7cab210f31a3b9952de4ece2746f19a50812fe05))
* **export-v2:** Test export localizations of single type localized ([da8c434](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/da8c4344d104705e7eed5e3d226855eab57f0b8f))
* **export-v2:** Test export of single type localized ([0fd1b9e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0fd1b9e9c39f93d1fda62cf7c3a96f60d8dd5b03))
* Generate unique data ([22a26e5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/22a26e5066383380e1fded3b925d66313e6fa235))
* **import-v2:** Should create or update standalone components ([97fb6b1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/97fb6b140345caa0b412f7e8f696b93731fcc349))
* **import-v2:** Should fail to import when missing required field ([b4407c0](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b4407c0e8036881d458fb9c5d0303c8790454bf3))
* **import-v2:** Should import collection type localized ([32f552a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/32f552a9954e0c8c9af5778a4621b1fb4bae072c))
* **import-v2:** Should set components for simple single and collection types ([372f1e4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/372f1e4a437ed7b0dc5965bf1c821c9b92cf644d))
* **import-v2:** Should update partially single types localized ([4618509](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4618509adda06e08f58f95741ec3fce778a5c380))
* **import-v2:** Test import of single type ([6092b67](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6092b67c6088d86e660a6194bee679c9323be0bb))
* **import-v2:** Test partial update of collection type ([3a2e8b3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3a2e8b393936846b13bb583e0b5d701d5a18b4fd))
* **import:** Comment limitation about single type localized ([a5576c1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a5576c1897cb1435e31ad35ceff949b994e7e225))
* **import:** Import single and collection types ([3f068dd](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3f068dd8ae8ddca8255bafd2a0b83ea80696455a))
* **import:** Remove irrelevant tests ([ef0f97a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ef0f97a37fca494612e499977587bbb800be6ec9))
* **import:** Set restaurant name unique ([99f1221](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/99f1221f7c6ba53647daf909353f6641c1be08c6))
* **import:** Setup import file ([b3e14bb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b3e14bb9bfa4036570c0b0fd9536a570a87a957e))
* **import:** Should create or update single type ([6aad02e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6aad02e1e6c3b3b5f70391294f8d95f1a51bb600))
* **import:** Should import relations in any order ([9ffaad0](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9ffaad0b4340ec5e8d708596c618c29a96171b28))
* **import:** Test update data ([8b47c9f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8b47c9fbe81c783506b9ba30e48a329b30089a37))
* **jest:** Update globals ([de2383e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/de2383ec5bd3340485fa68bfe9372424ec99bab4))
* **package.json:** Update dependencies ([fb89c4e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fb89c4efcbfd58f80392d630ceff857c080739c4))
* **strapi:** Cleanup components ([b86edaa](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b86edaaee3046310acfb20ccbc96856a4c77ad78))


### ⚡️ Features

* 🎸 Added uk locale ([0049591](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/00495915d50281901b87f1ebe07c8e51b4b6a24c))
* **admin:** Control access ([03b658a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/03b658af9daee04ee57e9077dae53e1a3e86692d))
* **admin:** Display plugin homepage ([75b2569](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/75b25698fca9e119e4616f2157787f8246e799d5))
* Create endpoints to import/export data from content api ([5b3445e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5b3445ef78cb34204511d090bd267574eef03aff))
* **doc:** Add section about filtering and sorting exported data ([86e26cf](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/86e26cf96f946ad9eae55ead3a4cb5335baedc49))
* **doc:** Detail how to import media programmatically ([4297ce0](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4297ce0ee4cf039178164ef66b7d74409cbf860b))
* **doc:** Update section to import media ([9fd57e2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9fd57e2df76cc3c56d6386fdb5c96d1accbfdd67))
* **edit-view:** Hide plugin section ([f5a7998](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f5a79988f694c2073984b2ae511ced1abf006e6e))
* **export-v2:** Export locales ([103bfd6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/103bfd655daddeff77d63f13d76bebfc826c708c))
* **export-v2:** Export single type ([6c3e54b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6c3e54bc417e6dc7b39e2afb732d32aa43744b2e))
* **export-v2:** Export whole database ([31e706a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/31e706afcf5e6c7fe9207b22572bb4acfb24a856))
* **export-v2:** Support exporting plugins content types ([b848262](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b848262af36bac5607518a3d54a1cadfa5571cea))
* **export:** Add deepness option ([71413a1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/71413a13cf6317c0e48aa3e3aac120d8a26a16ab))
* **export:** Enable use of version 2 ([d9914eb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d9914eb29cd3789b839e7ddcec42f79fd5161a14))
* **export:** Export media with absolute url ([0f3cc9b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0f3cc9b783a1f40aa290e9f6bbc644193892bfee))
* **export:** Export relations as id ([c8f4955](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c8f4955cc5357f4d0184bc3eaed349cf7019f32c))
* **export:** Export single types ([8d5fea1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8d5fea1a1b9e9b3d44e0f4f4622ea308bce9bf87))
* **export:** Implement v2 ([dbef311](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/dbef3110e375714532d3f89de5f2b3573f217438))
* **ExportModal:** Expose option to export plugins content types ([8cdf046](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8cdf0460d514daf331f1893dce28b976861206b1))
* **ExportModal:** Mark csv export as deprecated ([fbc64db](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fbc64dbc4f691c1ca81e067b1b1c810cfefa2cb3))
* **ExportModal:** Set json v2 as default export format ([3783185](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/378318573d6fade3a2bb9fe9dc903da1a94db84d))
* **export:** Parametrize export deepness ([bba4fac](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/bba4face6215b9e8e962a04968a70bb7d1cf25c8))
* **export:** Support deepness when export from content-api ([91f12de](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/91f12de78067585adbe90ddf65cf054754ec3119))
* **export:** Support programmatic media export ([79dca02](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/79dca0226f8e7010897d70a517936a26c69b23bf))
* Expose import logic in service ([7b575b9](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/7b575b93a24b94e2b8a5f7c0a1c2e6767e977daf))
* **file:** Import with metadata ([8dafa56](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8dafa5690c6dee3392c10ab3a43548ebb6c46592))
* **file:** Set id of imported file ([ad4ea5d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ad4ea5dd71222e4b80fad8bd9f94a71791149cb8))
* **get-model-attributes:** Add id field as option ([e0eabbd](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e0eabbd567cc2d09f5be4c7d12039efec018de59))
* **HomePage:** Add help section ([fa08af1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fa08af17078508a374730d1c49bb9b16ee9fd36a))
* **HomePage:** Add link to product roadmap ([65d1fe7](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/65d1fe7e1b85b380bad1665d72eb3f2246b2e648))
* **HomePage:** Setup basic layout ([022b363](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/022b363323862a778cb89f3b5fed8abef3338158))
* **i18n:** Shorten translation ([abfbba3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/abfbba307847ff8fa1c2c9b0a1976e614eeaab87))
* Import / export data according to user permissions ([6aee52d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6aee52d795aea6fcc99a869e67e7933347d56847))
* **import-data:** Support jso format ([e4774fe](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e4774fe7b81dd796c9278b07db5f7e6afc2435cd))
* **import-v2:** Import components and dynamic zones ([d93961d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d93961dc97820c69128b82acf8e3dfba62b5185b))
* **import-v2:** Import media of components ([131294b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/131294b875d4788055e92f9c95af757bfa7c0106))
* **import-v2:** Improve error understanding ([a9d576a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a9d576abd44f09cc32fd8ca16544cff77912fed4))
* **import-v2:** Support different id field per collection ([5c7da99](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5c7da999baba93ecc014deb484150c909540fd29))
* **import-v2:** Support partial import for components and dynamic zones ([40fd687](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/40fd68773fac81e2bca1920e824274bbc7bdc4fd))
* **import:** Enable use of v2 ([1494612](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/149461229a1c319742cae227d24c0b72f155f9de))
* **import:** Implement v2 ([568a0a8](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/568a0a86e96a24028a1f8454c964dd8d0b500d28))
* **import:** Import single type ([dcc5cae](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/dcc5cae627e100032c1797dec8d40f3653ef04e8))
* **ImportModal:** Add code editor as data source ([d4347d4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d4347d4c4bec9e576b67de72f4d8a113437acc2d))
* **ImportModal:** Display sucess/partial failure states ([083ab31](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/083ab3192502806fe9626cc1dad56a1ce8e8537f))
* **ImportModal:** Refresh view on import success ([602a4d3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/602a4d3c6a88859301939765b38cf2822acb6052))
* **import:** Select field used as identifier ([638115c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/638115ce4df3012b5fa405a85d4838cdfc7e2022))
* **import:** Support programmatic media import ([4da97e6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4da97e6b10d679cf9af4b65cc108f9ffd82e571c))
* Inject import/export buttons into single type edit page ([105b088](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/105b088edfc8fafff91f285e5c395ed2a522a1ce))
* **package.json:** Enable plugin for node 18 ([eb532cf](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/eb532cf6965b01a104ceb93169d8ea7cb86ddb24))
* **package:** Define files to pack ([a3d32ec](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a3d32ec51c3ddf76808a58742d1f3b05d4c0b946))
* **README:** Add access control section ([026983d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/026983dc219d742adcfa4e8aff84ceba5f4f640c))
* **README:** Add contribute section ([bfabc11](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/bfabc11d4a1973924e9fb3e1c2121b95ca0196e5))
* **README:** Add feedback and related plugin sections ([3117c99](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3117c99d96fe76134bd3a9d616bf82c47482cd01))
* **README:** Add link to product roadmap ([3839da4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3839da4d728216507f836ea48eb9cebef03395f9))
* **README:** Detail json v2 file structure ([91eda48](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/91eda48eadafe3dfbe96557721d98f2ba7b466cc))
* **README:** Explain use of idField per collection ([6222f5b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6222f5bf40acc15816fe5f6f9d87f9aa7c6e3548))
* **README:** Improve ([560d06a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/560d06a474ef2c656ce25ea59a7b88b6b79e1c77))
* **README:** Update illustration ([396b9cb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/396b9cb8e0405b00e2d2dbc997d7705ddbffb1c3))
* Setup tests ([8d044cf](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8d044cf1e53a12f7c193c54e4e46855606c015bc))
* Test export v2 of single and collection types ([939a3c2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/939a3c2b51428962bfc736c05ae746e51f43c2dd))
* **test:** Setup database before tests ([931566d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/931566d7ce2d329601afd5a8870a141d16744826))
* **test:** Test export of single type ([0d2f052](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0d2f052df8470eac9a28c88276fd47fed827692c))


### 🧹 Chores

* **.nvmrc:** Use lowest acceptable node version ([d1f43f1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d1f43f1b117a5b58d3c7c61bc148561fb4e9c1e7))
* **.versionrc.json:** Update commit keyword ([e7e35af](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e7e35af98903643463896303ba5438e4713a1f29))
* Add github CI ([d2f24cf](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d2f24cf713b9cce0931dd5addffc6c523ac81896))
* **admin:** Remove unused component ([89a283b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/89a283b50bc7fab3720921abf207c3012d18466c))
* **Alerts:** Clean imports ([252c09f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/252c09fb0054e4c0a2da455df0b83cae8ac07dba))
* **CHANGELOG.md:** Lint ([cc77582](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/cc775825c7aea32d3978ac86a2920be79e12196b))
* **CHANGELOG.md:** Prettify ([22eb453](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/22eb4532e0e957d16b957d4216f933b44918235b))
* **CHANGELOG:** Lint ([0801af5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0801af59a0a43da05f218336123ad82c52c9114e))
* Clean code ([0c6ff42](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0c6ff42e709487fa43d29ed1e5cbbda5fff06ab1))
* Clean project before rebuild ([bdf5d8d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/bdf5d8da04403117620c1b9a9d14a2601f34c67e))
* **dependencies:** Update ([dde8767](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/dde87674559f827ac516f3ea08aad56b806e190e))
* **doc:** Add feature ([37a01cb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/37a01cb8929cc14ed47688abcc98e3a4ff65733f))
* **doc:** Add import example in usage section ([b9a4a18](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b9a4a18d71d97dd563d69d816fc5d5211651e4bc))
* **doc:** Add screenshots ([194364e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/194364e7565d7acd8548b92fb45e3baa34947d8c))
* **doc:** Add webhook section ([1fe7db6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1fe7db6845be918e32b7f3779efa28f72636d381))
* **doc:** Clarify plugin description ([0e0c6e5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0e0c6e5fe0d01c6d8803d35cea24b2a928b58559))
* **doc:** Create content api section ([5592a21](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5592a21ee950edab49e6934135cc3b2afc5c0e94))
* **doc:** Describe how to solve the Payload too large error ([7eb3511](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/7eb3511aa4f16e827d3a4fef60cedf24e5b932ca))
* **doc:** Explain how to export whole database and setup preferences ([045c17b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/045c17be383b8b43120a7f055f79b5e9b82555a5))
* **doc:** Explain how to import data ([fa462bc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fa462bc757a88b3de898c9851d829ba0a477ec2c))
* **doc:** Improve ([6fea3ee](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6fea3ee3ef7b8ce99392ce7dafcfd04190e2c9cb))
* **doc:** Move features to the top ([a648096](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a648096ee91f221491212b622067df9686f65e99))
* **doc:** Tweak ([981354d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/981354d554a23156b221cd0f8550f36455ec0584))
* **doc:** Udpdate known limitations section ([e0e95ff](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e0e95ff75b4c49486b0daa463668b1f521d06e14))
* **doc:** Update ([c160751](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c160751c4927a80e9a1d12eefd05f3eb234d5b88))
* **doc:** Update ([fca65c4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fca65c4d91ec17d540ee371169925716471d3f31))
* **doc:** Update ([aa34ff6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/aa34ff636dc6b54538fec74426acbd0e62a7649f))
* **doc:** Update config section ([af55ae4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/af55ae41bbe8bd02a68664d1d69f043a217ebbfc))
* **doc:** Update import data section ([f84ac7e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f84ac7edb234f6f8a88ca521afb622ecb3be8939))
* **doc:** Update installation process ([9f71a53](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9f71a5338cea53cf0b7f99a9213795b530788e2b))
* **doc:** Update installation section ([df9b8ee](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/df9b8ee4b95c473625f34466c0a73b9bbb57430f))
* **Editor:** Increase size ([de886da](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/de886dab2e5c1d4e9360042f9ad600d7002af4b4))
* **eslint:** Sort imports ([140ac0e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/140ac0e310f07c57c920e5954628e73bbd93c896))
* **ExportModal:** Clean imports ([d3d91c8](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d3d91c85076d8ac19cc88eb09044295d18f6e0e1))
* **ExportModal:** Update wording ([242ef0f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/242ef0f87fc0a8d7fdcef1152b9f373d5aef27d2))
* Generate lock file ([d952282](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d95228233b184d7389663b37ea8558f2f1c2ca58))
* **github:** Run CI for pull request ([b66bb6d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b66bb6d6553634ad3e6b79f83e91352c1b7380fe))
* **github:** Update CI ([099880d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/099880d04bea25f2b1fb562ff3c57b25762ebb6c))
* **git:** Update gitignore ([55c3145](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/55c31458e40587bec72b77469cdafaf13437cb9a))
* **husky:** Send back to APA ([efbb795](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/efbb79554b0999c24dfbf9c3ba100dba29b24b90))
* **husky:** Setup husky ([dcfbb35](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/dcfbb35eb53b649704f719c86436d5cbddf03277))
* **import:** Improve doc ([14a8abe](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/14a8abea3841c0a68116aa10ad9235915c338d04))
* **InjectedImportExportSingleType:** Clean ([9fb740e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9fb740e6074cde771c9e7e9df282771fe1d42275))
* **jest:** Increase test timeout ([f68c45a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f68c45a7f76c1a8382d768b49408e96b69af1389))
* **libs:** Create method to check if object empty ([22b400f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/22b400f72b0c113bfc4fd686f2f0562e13a013dc))
* Lint CHANGELOG ([4c5afab](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4c5afabd4f20af0506dc3233d699e820f1000489))
* Lint files ([acf9b0a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/acf9b0a005bf82b76ae5c49161d6d2577474c598))
* Lint project ([47eec1c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/47eec1c7057ead26ca2bec37db66d1eca2bc16d7))
* Lint project ([d4bd0a9](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d4bd0a9d20f48dc354b6e6b2e670731ed13a3cf3))
* Lint project ([ea58c99](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ea58c99592b0bac2c3456ee0e664239503984f11))
* Lint project ([335dfed](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/335dfed396be0a8884f714ced69241add0ee7f95))
* **lint:** Configure linter and prettier ([4f250af](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4f250af7f3491bcb63b2e38bbc6dbdfb29e9ff84))
* Log error when import fails ([050a1b3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/050a1b35e714774a98edb502745d2886479cdb11))
* **objects:** Create helper to log objects ([370fc37](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/370fc37abfa3e893be58791c79e084dbc1277dbb))
* **package-lock.json:** Fix dependencies vulnerabilities ([d9bd8bc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d9bd8bc3ba429e5242169639d3b019014ca641eb))
* **package-lock.json:** Update node engine ([1dfff19](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1dfff19112c25a85635b1afc5b8cc171f411a89b))
* **package.json:** Build lib before publishing ([f30d054](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f30d054f4863fd3bcb71e8f87f3c8b477e8775d2))
* **package.json:** Update scripts ([29e3334](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/29e3334475c1466a68673110bf8a971fb16f66da))
* **package:** Add standard version ([24739b1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/24739b11af04019ec9684db0805c80f4c56f08fd))
* **package:** Update plugin description ([10395a6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/10395a6e442ae6571772cee07979053fa673c3bf))
* **parsers.js:** Delete file ([96438b1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/96438b11888ddb35a1f333066deb4e511531e0c5))
* **README.md:** Add section about known limitations ([2fcfeae](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/2fcfeaec58442d94caccf940d92d1b906381caaa))
* **README:** Enrich Installation section ([746a567](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/746a56756d1e44e0328d7c1fa9d41009444fac3c))
* **README:** Fix display ([425dc44](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/425dc44db8a3b95d6a6c6a5bf27a505846e9b881))
* **README:** Fix typo ([d3de155](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d3de155928acc55730c72659a89a2876cf5b7747))
* **README:** Improve ([f4cd346](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f4cd346affb5442d36f98115a2274f158364cce8))
* **README:** Update wording ([87536ef](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/87536ef83c3d89889e73bdec28ae09d8b9d4b475))
* Release v1.19.2 ([57c0870](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/57c0870023425d2e8a25d4f02346bf9724c4ffcd))
* Release v1.19.3 ([57157b7](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/57157b718ffedd36ddf153c17e2c8a8d6880ad73))
* Release v1.20.0 ([509de28](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/509de283e0cbcd6c1dbbf67fa4e261df88591474))
* Release v1.21.0 ([632636a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/632636a7336193980e45a9a440dc92ba653cac40))
* Release v1.21.1 ([cfdc7c0](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/cfdc7c0374447b626f59bda996ada33129266941))
* **release:** 1.17.0 ([80d134b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/80d134b591d76356696c0b01c082a2d6ab2cb66d))
* **release:** 1.17.1 ([3c6e0b8](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3c6e0b8d2a1f56440606f2e2733c26a9bdaef49e))
* **release:** 1.17.2 ([425dc77](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/425dc77dc98e27b71a9ecf64e9179075d9749df0))
* **release:** 1.17.3 ([01b356f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/01b356ffeb15d3f922aa6dedd5aeb4952b5f62b0))
* **release:** 1.18.0 ([44112f2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/44112f2aa780743694ae922cc9c73e15d0545007))
* **release:** 1.18.1 ([5599104](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5599104e651063169d6df7a5325add0492eb4fdb))
* **release:** 1.19.0 ([0fb61e6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0fb61e6b7e37018e392062209691abf19946c297))
* **release:** 1.19.1 ([9c41327](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9c41327d26c4687055f70f824c14ab30a5e7faae))
* **release:** 1.22.0 ([40483f2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/40483f233346eef7f808da2343948f41a03a8c1d))
* **release:** 1.22.1 ([decf851](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/decf851cbfad16025197c603cc5b067778a7625c))
* **release:** 1.22.2 ([f483419](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f48341931dc95b428d0bcaac85b7cf38b14be6ba))
* **release:** 1.23.0 ([ee7fc35](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ee7fc354f4aa008fac2aa72ba67bd92322ca06f9))
* **release:** 1.23.1 ([da0a5d3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/da0a5d3633faad041e8d410a621783fdc0535489))
* **release:** 1.24.0 ([3d12aab](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3d12aab96c77fe047343dc4665fca125da0b380e))
* Remove and ignore build folders ([f5b649a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f5b649a4287b60eb1f85f3b4c3b6b62929f399bf))
* Set node version ([6495c11](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6495c118b49df88eaf1e1de772626839e8f18d5d))
* Set plugin display name ([9d6a801](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9d6a801537f64fc012564cd0ade3b3274caf9b27))
* **settings:** Match latest vscode version ([cc8a328](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/cc8a3283bc8597bb84c90fbffb13b8a967637c97))
* Setup nodemon to hot rebuild ([982841f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/982841f49c5563de3c1eb5379442aa23f6a446a0))
* Setup ts jest ([fdd406b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fdd406b14bc46da9e0c71a1aadc94db1c38adfcd))
* Setup typescript for server ([375a341](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/375a341aacd0a8844948f11439ad191509a8e85c))
* Support node v20.x.x ([1c8f591](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1c8f5917cfac12129af1be1934736694f61d7708))
* Switch to yarn ([4531386](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4531386c6f96b65d74279b256fb935b6da4112d6))
* **test:** Load strapi plugins ([f43609b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f43609bac660134e621d9cf1b461dfa5552024df))
* **tests:** Clean test app ([5244713](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5244713f37e1b0ed4de6cebb49d4cfa189e905aa))
* **test:** Tweak config to debugging ([de4300a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/de4300ae1d730cc9374857b2236ed288b0d4b727))
* **tsconfig:** Update module import ([69cd79b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/69cd79bc8b80b97f6da7e30d9e7339e9cab21727))
* Update issue templates ([93756f0](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/93756f00f662adb787da83d3f7fd32d5eb56b5a2))
* Update node version ([8b24d01](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8b24d01507c570a94eefffc1c04c0efc8bbe6cca))
* Update node version ([8d7ee7f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8d7ee7f476935f151ad6d1888a75c24b8d88fb0e))
* Upgrade strapi version ([134adc2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/134adc2a5a60dc718284ee4d3166d897a82e1830))
* Use npm ([d2c39e1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d2c39e116a97c66f5968fafa6649cb6596f7a68d))
* **vscode:** Recommend extensions ([d5fb201](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d5fb2016e202fb318a38f8b77b763bfa9e132bb2))

## 1.24.0 (2025-09-02)


### 🛠 Refacto

* **admin:** Rename components ([876bce4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/876bce4de9ab58125d075e2c019ae5e245e702f5))
* **api:** Simplify ([968899a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/968899a0fd1afacc9f59d3c1f7b8e2f953ed4348))
* **converters-v2:** Move to ts ([9340b38](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9340b38354f04c09a7cfa2f7c093b880439f9c02))
* **export-v2:** Move to ts ([69e86a5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/69e86a5ef74d862e0b7d6e8d2bb1d64e66a5da3c))
* **import-v2:** Map file ids to database ids ([e0b9383](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e0b9383c344a3328383bf8c562513041f703baa0))
* **import-v2:** Move to ts ([f492927](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f492927d2f83c08466e6f38899f20bfdf2adf051))
* **ImportEditor:** Translate component ([52b0303](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/52b0303e39057836cecf5ccd7b9615a3f1906406))
* **models:** Move to ts ([c9dadef](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c9dadef6ac4d511f3e8927b99d340ec0cecedcfb))
* **test:** Rename test files ([5e9f695](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5e9f69566028e47de09f90d2a2d88cc4c71dcc49))
* **tests:** Move tests to specs folder ([6f0f85c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6f0f85c73999e38bde82765b418b32789cf27876))


### 🐞 Bug Fixes

* **admin:** use AnErrorOccurred instead of missing NotFound ([ac993e7](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ac993e7b911513e48660694a6a8711a92bd46b5a)), closes [#129](https://github.com/Baboo7/strapi-plugin-import-export-entries/issues/129)
* application/vnd.ms-excel not supported. ([2ff0804](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/2ff0804b33379ba29c3cc2a8c8b9e68882d33051))
* back to node-fetch v2 ([aa018cc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/aa018cc5028e8f36d10adcae75193a7e068557c5))
* **convert-data:** Export all entity attributes ([edd8a18](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/edd8a18a65ef2dec5fecd607867843e202c756c4))
* **convert-data:** Export relations as id ([bb0e540](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/bb0e540755bbc24fab5e9dd2332cf709fddb7625))
* **convert-data:** Stringify objects when export in csv ([b2106bc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b2106bcc01b9c072b907b5cb233728416f86575d))
* Define const ([1cd1585](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1cd1585c269a73a1ec04366b1d66303bb6629901))
* **doc:** Make relations import clearer ([cf88780](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/cf887805be3f8194f8c86763e0e14607c0ecb7cb))
* **doc:** Set anchor to section ([435b5f9](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/435b5f9b8b3b34166d35d755ac64184ac50a01ff))
* **Editor:** Load monaco editor locally ([6bf1e15](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6bf1e1594fb58fb068bff04a6bfc64288d88da77))
* **eslint:** Add jest config ([8f2d37c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8f2d37c63be61c2443b8d834a56e5359327b783b))
* **export-v2:** Export components for simple single and collection types ([c954791](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c9547913b4699da87fac2f5d16345a59669600f7))
* **export-v2:** Export selected slugs ([a9482bd](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a9482bd24286a190df4d73f9a5f0416eb6c027dd))
* **export-v2:** Export single type localized ([09dc3cc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/09dc3ccaac27921c6c5f2c9fc822886784c3eaf3))
* **export-v2:** Filter out ids using attribute slug ([4e6e4e7](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4e6e4e77c3aa63b223a67902ad99c3baacf99835))
* **export-v2:** Remove duplicated components in dynamic zones ([eb7ce26](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/eb7ce26c8fd10b2d418ea1e117248db479656f71))
* **export:** Adapt sort query to entity service ([94c77eb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/94c77eb02aeff92d0c60d9bc8848944ac72bfee3))
* **export:** Apply filters fix, replaceAll replaced with replace method for CSV conversion ([696254d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/696254d5ce4f1432e31ab0bed06d76cb9070b46a))
* **export:** Export dynamic zone ([635dc72](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/635dc724cad3652ecb8db6fd22faead815464d11))
* **export:** Handle export relations as ids for one to many relations ([44f25b5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/44f25b5ccd46ae60c28e32eb480b38411291379b))
* **export:** Handle null when export relations as id ([b01d246](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b01d2460f10b4768389736b2ff3a722b7c4ea69c))
* **export:** Sub populate seo component ([ab5d227](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ab5d227bffa0592bce855ebdc7ce2d655edaf55e))
* **file:** Set empty caption and alternative text ([b9ad57c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b9ad57c4dfc645ac6707e7e9a9961080eb3d6aa9))
* **file:** Set right file name ([113fed1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/113fed10f9df8822fe94ddd6979ea3eb2b2a60d4))
* **gitignore:** Ignore only folders at project root ([8a172ab](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8a172ab0540e379606b8a89495534b7b4532d1b3))
* Have component export/import compatible with entity service ([5006837](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5006837327e63fd48322a177a63ee7aab5d8f1bb))
* **HomePage:** Display alerts ([8b55716](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8b557162a1f183e15b9d3fc53e346ef94986dbba))
* **import-data:** Check content permissions when import whole database ([c5c8727](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c5c8727285f12f8b086300f1807038611ee53474))
* **import-v2:** Fail when missing required field ([fda6ca2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fda6ca2c856a29cb88f1d93ba08925b3bd71b989))
* **import-v2:** Ignore slugs with no associated schema ([c6efb54](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c6efb5461b08265173f68bc312b13dc5b9d752b9))
* **import-v2:** Import collection types localized ([4a2c913](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4a2c9131a27e1c3f3e0175cc3aa25cf7751014f3))
* **import-v2:** Import components and dynamic zones for not localized content types ([378a7e3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/378a7e37627c8e1451eadac28ae0b5c431308442))
* **import-v2:** Import media files once ([f0a4518](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f0a4518421a11968f0c6903d4f4088c680ba1e9b))
* **import-v2:** Import single type ([35b54c5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/35b54c5d4ec730f9159059589a0ba1c522b36565))
* **import-v2:** Import single type with specified id ([1e66da1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1e66da1a4500f20acd27d17b4ad2c66d6d806cbf))
* **import-v2:** Set components for simple single and collection types ([366461f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/366461f6c548cc808d2a0fd4823c333e58d08d99))
* **import-v2:** Sync primary key sequence for postgres database ([5220b64](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5220b6456169be6357c2e092cec13c0e4f8c310a))
* **import:** change deprecated request by node-fetch module ([f1e63bb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f1e63bb27095e2922e7eae9395e1f2e79f6b4056))
* **ImportEditor:** Make import options more obvious ([e13fa70](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e13fa70b72d50fa451c4faa54bc127cf89e316c9))
* **import:** fallback to any when media field is missing allowedTypes ([0cce657](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0cce657d322ac39d737c74e0b7fc1897bab79b49))
* **import:** Handle import of one to many relations ([af6daab](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/af6daab950e71a3959d263c7d59981b6b92c6007))
* **import:** Import components and dynamic zones sequentially ([6d5f2f1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6d5f2f1f8ae13b5d067fb843d32b443c59bad3af))
* **import:** Import entries with dependencies ([413362b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/413362bc141ca096b28e36601ab787e7961f3eaf))
* **import:** Import media ([c8f0aad](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c8f0aad9d82c5951dd87739286ef49c435896a04))
* **import:** Import/Set media by id, url or object ([c85bfe2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c85bfe2c6a7999f540ab6c88021b477007b4bab2))
* **ImportModal:** Handle error payload too large ([2b0159f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/2b0159f67d9d3ed4c178f2c1081c78a3339bde97))
* **import:** Support import of dynamic zones ([1ec2b04](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1ec2b046aec5176f5741b9087bbe27f844b7b232))
* **import:** Support import of relations defined in any order ([9a481a5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9a481a58738c9f4c1554d8f4b1d8d2f561222e37))
* **import:** Use current admin user id for create/update fields ([fc0408a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fc0408ad3c772e44f7cad29bfd490a46b27a07b2))
* **package:** Only clean build folders on publish ([97e84cb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/97e84cb734ed98908e8af162a686c95d3576f0bb))
* **README:** Remove webpack config section ([41a32e6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/41a32e61038813f63f932caa37d773d8e70f908a))
* **README:** Update discord invite link ([4b1be2c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4b1be2c58a49b8a13610969ceb0218b4f983b63f))
* **test:** Await database cleanup ([f47fb89](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f47fb891cea3edaf9e7713c1fcc69dcd95a2365f))
* **tests:** Comment date tests temporarily for CI ([6f5128c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6f5128c76f48592d8c974eab31e3d8cb589503fe))
* **tests:** Reset auto increment sequence ([6ca9fbb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6ca9fbb425767b090563ea798a2139a527bf7c5e))
* **types:** Fix typing ([5c86e4f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5c86e4f5bc42a68c1e0d852c4092930729c4db84))
* Update plugin name ([20a4b6d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/20a4b6dd2dbefefea2be200ada3fb7160c9ae785))
* Update request urls ([f27b3c9](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f27b3c96769daa6b73ce0339017783e7335ad709))
* **useSlug:** Extract slug on collection type entry page ([8022306](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8022306c106e2dc9a09916721bb488b68f8c8d49))
* **utils:** Remove non existent exports ([6dfef02](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6dfef02e78296dc484523028ef8b81c70c8be63f))


### 🧪 Tests

* **collection-type:** Create localized collection type ([efa58f5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/efa58f52f66b883d447bc7725a11b6cfa95eea83))
* **config:** Define global strapi for ts ([de75c22](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/de75c22fe876eaae97e24b2d1063c3d2ccbc5b9f))
* **export-v2:** Should export collection type localized ([454e2d3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/454e2d345b0cadaca40e69289a8db4e762ccc6ab))
* **export-v2:** Should export components for simple single and collection types ([c1e4528](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c1e452841f68d3441caf560259e7a4b07ab53614))
* **export-v2:** Should export whole database ([7cab210](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/7cab210f31a3b9952de4ece2746f19a50812fe05))
* **export-v2:** Test export localizations of single type localized ([da8c434](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/da8c4344d104705e7eed5e3d226855eab57f0b8f))
* **export-v2:** Test export of single type localized ([0fd1b9e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0fd1b9e9c39f93d1fda62cf7c3a96f60d8dd5b03))
* Generate unique data ([22a26e5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/22a26e5066383380e1fded3b925d66313e6fa235))
* **import-v2:** Should create or update standalone components ([97fb6b1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/97fb6b140345caa0b412f7e8f696b93731fcc349))
* **import-v2:** Should fail to import when missing required field ([b4407c0](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b4407c0e8036881d458fb9c5d0303c8790454bf3))
* **import-v2:** Should import collection type localized ([32f552a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/32f552a9954e0c8c9af5778a4621b1fb4bae072c))
* **import-v2:** Should set components for simple single and collection types ([372f1e4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/372f1e4a437ed7b0dc5965bf1c821c9b92cf644d))
* **import-v2:** Should update partially single types localized ([4618509](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4618509adda06e08f58f95741ec3fce778a5c380))
* **import-v2:** Test import of single type ([6092b67](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6092b67c6088d86e660a6194bee679c9323be0bb))
* **import-v2:** Test partial update of collection type ([3a2e8b3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3a2e8b393936846b13bb583e0b5d701d5a18b4fd))
* **import:** Comment limitation about single type localized ([a5576c1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a5576c1897cb1435e31ad35ceff949b994e7e225))
* **import:** Import single and collection types ([3f068dd](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3f068dd8ae8ddca8255bafd2a0b83ea80696455a))
* **import:** Remove irrelevant tests ([ef0f97a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ef0f97a37fca494612e499977587bbb800be6ec9))
* **import:** Set restaurant name unique ([99f1221](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/99f1221f7c6ba53647daf909353f6641c1be08c6))
* **import:** Setup import file ([b3e14bb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b3e14bb9bfa4036570c0b0fd9536a570a87a957e))
* **import:** Should create or update single type ([6aad02e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6aad02e1e6c3b3b5f70391294f8d95f1a51bb600))
* **import:** Should import relations in any order ([9ffaad0](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9ffaad0b4340ec5e8d708596c618c29a96171b28))
* **import:** Test update data ([8b47c9f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8b47c9fbe81c783506b9ba30e48a329b30089a37))
* **jest:** Update globals ([de2383e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/de2383ec5bd3340485fa68bfe9372424ec99bab4))
* **package.json:** Update dependencies ([fb89c4e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fb89c4efcbfd58f80392d630ceff857c080739c4))
* **strapi:** Cleanup components ([b86edaa](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b86edaaee3046310acfb20ccbc96856a4c77ad78))


### ⚡️ Features

* 🎸 Added uk locale ([0049591](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/00495915d50281901b87f1ebe07c8e51b4b6a24c))
* **admin:** Control access ([03b658a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/03b658af9daee04ee57e9077dae53e1a3e86692d))
* **admin:** Display plugin homepage ([75b2569](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/75b25698fca9e119e4616f2157787f8246e799d5))
* Create endpoints to import/export data from content api ([5b3445e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5b3445ef78cb34204511d090bd267574eef03aff))
* **doc:** Add section about filtering and sorting exported data ([86e26cf](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/86e26cf96f946ad9eae55ead3a4cb5335baedc49))
* **doc:** Detail how to import media programmatically ([4297ce0](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4297ce0ee4cf039178164ef66b7d74409cbf860b))
* **doc:** Update section to import media ([9fd57e2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9fd57e2df76cc3c56d6386fdb5c96d1accbfdd67))
* **edit-view:** Hide plugin section ([f5a7998](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f5a79988f694c2073984b2ae511ced1abf006e6e))
* **export-v2:** Export locales ([103bfd6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/103bfd655daddeff77d63f13d76bebfc826c708c))
* **export-v2:** Export single type ([6c3e54b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6c3e54bc417e6dc7b39e2afb732d32aa43744b2e))
* **export-v2:** Export whole database ([31e706a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/31e706afcf5e6c7fe9207b22572bb4acfb24a856))
* **export-v2:** Support exporting plugins content types ([b848262](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b848262af36bac5607518a3d54a1cadfa5571cea))
* **export:** Add deepness option ([71413a1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/71413a13cf6317c0e48aa3e3aac120d8a26a16ab))
* **export:** Enable use of version 2 ([d9914eb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d9914eb29cd3789b839e7ddcec42f79fd5161a14))
* **export:** Export media with absolute url ([0f3cc9b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0f3cc9b783a1f40aa290e9f6bbc644193892bfee))
* **export:** Export relations as id ([c8f4955](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c8f4955cc5357f4d0184bc3eaed349cf7019f32c))
* **export:** Export single types ([8d5fea1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8d5fea1a1b9e9b3d44e0f4f4622ea308bce9bf87))
* **export:** Implement v2 ([dbef311](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/dbef3110e375714532d3f89de5f2b3573f217438))
* **ExportModal:** Expose option to export plugins content types ([8cdf046](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8cdf0460d514daf331f1893dce28b976861206b1))
* **ExportModal:** Mark csv export as deprecated ([fbc64db](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fbc64dbc4f691c1ca81e067b1b1c810cfefa2cb3))
* **ExportModal:** Set json v2 as default export format ([3783185](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/378318573d6fade3a2bb9fe9dc903da1a94db84d))
* **export:** Parametrize export deepness ([bba4fac](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/bba4face6215b9e8e962a04968a70bb7d1cf25c8))
* **export:** Support deepness when export from content-api ([91f12de](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/91f12de78067585adbe90ddf65cf054754ec3119))
* **export:** Support programmatic media export ([79dca02](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/79dca0226f8e7010897d70a517936a26c69b23bf))
* Expose import logic in service ([7b575b9](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/7b575b93a24b94e2b8a5f7c0a1c2e6767e977daf))
* **file:** Import with metadata ([8dafa56](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8dafa5690c6dee3392c10ab3a43548ebb6c46592))
* **file:** Set id of imported file ([ad4ea5d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ad4ea5dd71222e4b80fad8bd9f94a71791149cb8))
* **get-model-attributes:** Add id field as option ([e0eabbd](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e0eabbd567cc2d09f5be4c7d12039efec018de59))
* **HomePage:** Add help section ([fa08af1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fa08af17078508a374730d1c49bb9b16ee9fd36a))
* **HomePage:** Add link to product roadmap ([65d1fe7](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/65d1fe7e1b85b380bad1665d72eb3f2246b2e648))
* **HomePage:** Setup basic layout ([022b363](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/022b363323862a778cb89f3b5fed8abef3338158))
* **i18n:** Shorten translation ([abfbba3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/abfbba307847ff8fa1c2c9b0a1976e614eeaab87))
* Import / export data according to user permissions ([6aee52d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6aee52d795aea6fcc99a869e67e7933347d56847))
* **import-data:** Support jso format ([e4774fe](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e4774fe7b81dd796c9278b07db5f7e6afc2435cd))
* **import-v2:** Import components and dynamic zones ([d93961d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d93961dc97820c69128b82acf8e3dfba62b5185b))
* **import-v2:** Import media of components ([131294b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/131294b875d4788055e92f9c95af757bfa7c0106))
* **import-v2:** Improve error understanding ([a9d576a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a9d576abd44f09cc32fd8ca16544cff77912fed4))
* **import-v2:** Support different id field per collection ([5c7da99](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5c7da999baba93ecc014deb484150c909540fd29))
* **import-v2:** Support partial import for components and dynamic zones ([40fd687](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/40fd68773fac81e2bca1920e824274bbc7bdc4fd))
* **import:** Enable use of v2 ([1494612](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/149461229a1c319742cae227d24c0b72f155f9de))
* **import:** Implement v2 ([568a0a8](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/568a0a86e96a24028a1f8454c964dd8d0b500d28))
* **import:** Import single type ([dcc5cae](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/dcc5cae627e100032c1797dec8d40f3653ef04e8))
* **ImportModal:** Add code editor as data source ([d4347d4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d4347d4c4bec9e576b67de72f4d8a113437acc2d))
* **ImportModal:** Display sucess/partial failure states ([083ab31](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/083ab3192502806fe9626cc1dad56a1ce8e8537f))
* **ImportModal:** Refresh view on import success ([602a4d3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/602a4d3c6a88859301939765b38cf2822acb6052))
* **import:** Select field used as identifier ([638115c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/638115ce4df3012b5fa405a85d4838cdfc7e2022))
* **import:** Support programmatic media import ([4da97e6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4da97e6b10d679cf9af4b65cc108f9ffd82e571c))
* Inject import/export buttons into single type edit page ([105b088](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/105b088edfc8fafff91f285e5c395ed2a522a1ce))
* **package.json:** Enable plugin for node 18 ([eb532cf](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/eb532cf6965b01a104ceb93169d8ea7cb86ddb24))
* **package:** Define files to pack ([a3d32ec](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a3d32ec51c3ddf76808a58742d1f3b05d4c0b946))
* **README:** Add access control section ([026983d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/026983dc219d742adcfa4e8aff84ceba5f4f640c))
* **README:** Add contribute section ([bfabc11](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/bfabc11d4a1973924e9fb3e1c2121b95ca0196e5))
* **README:** Add feedback and related plugin sections ([3117c99](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3117c99d96fe76134bd3a9d616bf82c47482cd01))
* **README:** Add link to product roadmap ([3839da4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3839da4d728216507f836ea48eb9cebef03395f9))
* **README:** Detail json v2 file structure ([91eda48](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/91eda48eadafe3dfbe96557721d98f2ba7b466cc))
* **README:** Explain use of idField per collection ([6222f5b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6222f5bf40acc15816fe5f6f9d87f9aa7c6e3548))
* **README:** Improve ([560d06a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/560d06a474ef2c656ce25ea59a7b88b6b79e1c77))
* **README:** Update illustration ([396b9cb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/396b9cb8e0405b00e2d2dbc997d7705ddbffb1c3))
* Setup tests ([8d044cf](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8d044cf1e53a12f7c193c54e4e46855606c015bc))
* Test export v2 of single and collection types ([939a3c2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/939a3c2b51428962bfc736c05ae746e51f43c2dd))
* **test:** Setup database before tests ([931566d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/931566d7ce2d329601afd5a8870a141d16744826))
* **test:** Test export of single type ([0d2f052](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0d2f052df8470eac9a28c88276fd47fed827692c))


### 🧹 Chores

* **.nvmrc:** Use lowest acceptable node version ([d1f43f1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d1f43f1b117a5b58d3c7c61bc148561fb4e9c1e7))
* **.versionrc.json:** Update commit keyword ([e7e35af](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e7e35af98903643463896303ba5438e4713a1f29))
* Add github CI ([d2f24cf](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d2f24cf713b9cce0931dd5addffc6c523ac81896))
* **admin:** Remove unused component ([89a283b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/89a283b50bc7fab3720921abf207c3012d18466c))
* **Alerts:** Clean imports ([252c09f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/252c09fb0054e4c0a2da455df0b83cae8ac07dba))
* **CHANGELOG.md:** Lint ([cc77582](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/cc775825c7aea32d3978ac86a2920be79e12196b))
* **CHANGELOG.md:** Prettify ([22eb453](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/22eb4532e0e957d16b957d4216f933b44918235b))
* **CHANGELOG:** Lint ([0801af5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0801af59a0a43da05f218336123ad82c52c9114e))
* Clean code ([0c6ff42](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0c6ff42e709487fa43d29ed1e5cbbda5fff06ab1))
* Clean project before rebuild ([bdf5d8d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/bdf5d8da04403117620c1b9a9d14a2601f34c67e))
* **dependencies:** Update ([dde8767](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/dde87674559f827ac516f3ea08aad56b806e190e))
* **doc:** Add feature ([37a01cb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/37a01cb8929cc14ed47688abcc98e3a4ff65733f))
* **doc:** Add import example in usage section ([b9a4a18](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b9a4a18d71d97dd563d69d816fc5d5211651e4bc))
* **doc:** Add screenshots ([194364e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/194364e7565d7acd8548b92fb45e3baa34947d8c))
* **doc:** Add webhook section ([1fe7db6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1fe7db6845be918e32b7f3779efa28f72636d381))
* **doc:** Clarify plugin description ([0e0c6e5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0e0c6e5fe0d01c6d8803d35cea24b2a928b58559))
* **doc:** Create content api section ([5592a21](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5592a21ee950edab49e6934135cc3b2afc5c0e94))
* **doc:** Describe how to solve the Payload too large error ([7eb3511](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/7eb3511aa4f16e827d3a4fef60cedf24e5b932ca))
* **doc:** Explain how to export whole database and setup preferences ([045c17b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/045c17be383b8b43120a7f055f79b5e9b82555a5))
* **doc:** Explain how to import data ([fa462bc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fa462bc757a88b3de898c9851d829ba0a477ec2c))
* **doc:** Improve ([6fea3ee](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6fea3ee3ef7b8ce99392ce7dafcfd04190e2c9cb))
* **doc:** Move features to the top ([a648096](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a648096ee91f221491212b622067df9686f65e99))
* **doc:** Tweak ([981354d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/981354d554a23156b221cd0f8550f36455ec0584))
* **doc:** Udpdate known limitations section ([e0e95ff](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e0e95ff75b4c49486b0daa463668b1f521d06e14))
* **doc:** Update ([c160751](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c160751c4927a80e9a1d12eefd05f3eb234d5b88))
* **doc:** Update ([fca65c4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fca65c4d91ec17d540ee371169925716471d3f31))
* **doc:** Update ([aa34ff6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/aa34ff636dc6b54538fec74426acbd0e62a7649f))
* **doc:** Update config section ([af55ae4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/af55ae41bbe8bd02a68664d1d69f043a217ebbfc))
* **doc:** Update import data section ([f84ac7e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f84ac7edb234f6f8a88ca521afb622ecb3be8939))
* **doc:** Update installation process ([9f71a53](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9f71a5338cea53cf0b7f99a9213795b530788e2b))
* **doc:** Update installation section ([df9b8ee](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/df9b8ee4b95c473625f34466c0a73b9bbb57430f))
* **Editor:** Increase size ([de886da](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/de886dab2e5c1d4e9360042f9ad600d7002af4b4))
* **eslint:** Sort imports ([140ac0e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/140ac0e310f07c57c920e5954628e73bbd93c896))
* **ExportModal:** Clean imports ([d3d91c8](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d3d91c85076d8ac19cc88eb09044295d18f6e0e1))
* **ExportModal:** Update wording ([242ef0f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/242ef0f87fc0a8d7fdcef1152b9f373d5aef27d2))
* Generate lock file ([d952282](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d95228233b184d7389663b37ea8558f2f1c2ca58))
* **github:** Run CI for pull request ([b66bb6d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b66bb6d6553634ad3e6b79f83e91352c1b7380fe))
* **github:** Update CI ([099880d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/099880d04bea25f2b1fb562ff3c57b25762ebb6c))
* **git:** Update gitignore ([55c3145](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/55c31458e40587bec72b77469cdafaf13437cb9a))
* **husky:** Send back to APA ([efbb795](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/efbb79554b0999c24dfbf9c3ba100dba29b24b90))
* **husky:** Setup husky ([dcfbb35](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/dcfbb35eb53b649704f719c86436d5cbddf03277))
* **import:** Improve doc ([14a8abe](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/14a8abea3841c0a68116aa10ad9235915c338d04))
* **InjectedImportExportSingleType:** Clean ([9fb740e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9fb740e6074cde771c9e7e9df282771fe1d42275))
* **jest:** Increase test timeout ([f68c45a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f68c45a7f76c1a8382d768b49408e96b69af1389))
* **libs:** Create method to check if object empty ([22b400f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/22b400f72b0c113bfc4fd686f2f0562e13a013dc))
* Lint CHANGELOG ([4c5afab](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4c5afabd4f20af0506dc3233d699e820f1000489))
* Lint files ([acf9b0a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/acf9b0a005bf82b76ae5c49161d6d2577474c598))
* Lint project ([47eec1c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/47eec1c7057ead26ca2bec37db66d1eca2bc16d7))
* Lint project ([d4bd0a9](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d4bd0a9d20f48dc354b6e6b2e670731ed13a3cf3))
* Lint project ([ea58c99](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ea58c99592b0bac2c3456ee0e664239503984f11))
* Lint project ([335dfed](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/335dfed396be0a8884f714ced69241add0ee7f95))
* **lint:** Configure linter and prettier ([4f250af](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4f250af7f3491bcb63b2e38bbc6dbdfb29e9ff84))
* Log error when import fails ([050a1b3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/050a1b35e714774a98edb502745d2886479cdb11))
* **objects:** Create helper to log objects ([370fc37](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/370fc37abfa3e893be58791c79e084dbc1277dbb))
* **package-lock.json:** Fix dependencies vulnerabilities ([d9bd8bc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d9bd8bc3ba429e5242169639d3b019014ca641eb))
* **package-lock.json:** Update node engine ([1dfff19](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1dfff19112c25a85635b1afc5b8cc171f411a89b))
* **package.json:** Build lib before publishing ([f30d054](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f30d054f4863fd3bcb71e8f87f3c8b477e8775d2))
* **package.json:** Update scripts ([29e3334](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/29e3334475c1466a68673110bf8a971fb16f66da))
* **package:** Add standard version ([24739b1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/24739b11af04019ec9684db0805c80f4c56f08fd))
* **package:** Update plugin description ([10395a6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/10395a6e442ae6571772cee07979053fa673c3bf))
* **parsers.js:** Delete file ([96438b1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/96438b11888ddb35a1f333066deb4e511531e0c5))
* **README.md:** Add section about known limitations ([2fcfeae](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/2fcfeaec58442d94caccf940d92d1b906381caaa))
* **README:** Enrich Installation section ([746a567](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/746a56756d1e44e0328d7c1fa9d41009444fac3c))
* **README:** Fix display ([425dc44](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/425dc44db8a3b95d6a6c6a5bf27a505846e9b881))
* **README:** Fix typo ([d3de155](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d3de155928acc55730c72659a89a2876cf5b7747))
* **README:** Improve ([f4cd346](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f4cd346affb5442d36f98115a2274f158364cce8))
* **README:** Update wording ([87536ef](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/87536ef83c3d89889e73bdec28ae09d8b9d4b475))
* Release v1.19.2 ([57c0870](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/57c0870023425d2e8a25d4f02346bf9724c4ffcd))
* Release v1.19.3 ([57157b7](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/57157b718ffedd36ddf153c17e2c8a8d6880ad73))
* Release v1.20.0 ([509de28](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/509de283e0cbcd6c1dbbf67fa4e261df88591474))
* Release v1.21.0 ([632636a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/632636a7336193980e45a9a440dc92ba653cac40))
* Release v1.21.1 ([cfdc7c0](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/cfdc7c0374447b626f59bda996ada33129266941))
* **release:** 1.17.0 ([80d134b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/80d134b591d76356696c0b01c082a2d6ab2cb66d))
* **release:** 1.17.1 ([3c6e0b8](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3c6e0b8d2a1f56440606f2e2733c26a9bdaef49e))
* **release:** 1.17.2 ([425dc77](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/425dc77dc98e27b71a9ecf64e9179075d9749df0))
* **release:** 1.17.3 ([01b356f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/01b356ffeb15d3f922aa6dedd5aeb4952b5f62b0))
* **release:** 1.18.0 ([44112f2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/44112f2aa780743694ae922cc9c73e15d0545007))
* **release:** 1.18.1 ([5599104](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5599104e651063169d6df7a5325add0492eb4fdb))
* **release:** 1.19.0 ([0fb61e6](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0fb61e6b7e37018e392062209691abf19946c297))
* **release:** 1.19.1 ([9c41327](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9c41327d26c4687055f70f824c14ab30a5e7faae))
* **release:** 1.22.0 ([40483f2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/40483f233346eef7f808da2343948f41a03a8c1d))
* **release:** 1.22.1 ([decf851](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/decf851cbfad16025197c603cc5b067778a7625c))
* **release:** 1.22.2 ([f483419](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f48341931dc95b428d0bcaac85b7cf38b14be6ba))
* **release:** 1.23.0 ([ee7fc35](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ee7fc354f4aa008fac2aa72ba67bd92322ca06f9))
* **release:** 1.23.1 ([da0a5d3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/da0a5d3633faad041e8d410a621783fdc0535489))
* Remove and ignore build folders ([f5b649a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f5b649a4287b60eb1f85f3b4c3b6b62929f399bf))
* Set node version ([6495c11](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6495c118b49df88eaf1e1de772626839e8f18d5d))
* Set plugin display name ([9d6a801](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/9d6a801537f64fc012564cd0ade3b3274caf9b27))
* **settings:** Match latest vscode version ([cc8a328](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/cc8a3283bc8597bb84c90fbffb13b8a967637c97))
* Setup nodemon to hot rebuild ([982841f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/982841f49c5563de3c1eb5379442aa23f6a446a0))
* Setup ts jest ([fdd406b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fdd406b14bc46da9e0c71a1aadc94db1c38adfcd))
* Setup typescript for server ([375a341](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/375a341aacd0a8844948f11439ad191509a8e85c))
* Support node v20.x.x ([1c8f591](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1c8f5917cfac12129af1be1934736694f61d7708))
* Switch to yarn ([4531386](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4531386c6f96b65d74279b256fb935b6da4112d6))
* **test:** Load strapi plugins ([f43609b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f43609bac660134e621d9cf1b461dfa5552024df))
* **tests:** Clean test app ([5244713](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5244713f37e1b0ed4de6cebb49d4cfa189e905aa))
* **test:** Tweak config to debugging ([de4300a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/de4300ae1d730cc9374857b2236ed288b0d4b727))
* **tsconfig:** Update module import ([69cd79b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/69cd79bc8b80b97f6da7e30d9e7339e9cab21727))
* Update issue templates ([93756f0](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/93756f00f662adb787da83d3f7fd32d5eb56b5a2))
* Update node version ([8b24d01](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8b24d01507c570a94eefffc1c04c0efc8bbe6cca))
* Update node version ([8d7ee7f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8d7ee7f476935f151ad6d1888a75c24b8d88fb0e))
* Upgrade strapi version ([134adc2](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/134adc2a5a60dc718284ee4d3166d897a82e1830))
* Use npm ([d2c39e1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d2c39e116a97c66f5968fafa6649cb6596f7a68d))
* **vscode:** Recommend extensions ([d5fb201](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d5fb2016e202fb318a38f8b77b763bfa9e132bb2))

### [1.23.1](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.23.0...v1.23.1) (2023-12-27)

## [1.23.0](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.22.2...v1.23.0) (2023-12-23)


### ⚡️ Features

* 🎸 Added uk locale ([0049591](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/00495915d50281901b87f1ebe07c8e51b4b6a24c))

### [1.22.2](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.22.1...v1.22.2) (2023-12-09)


### ⚡️ Features

* **import-v2:** Import media of components ([131294b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/131294b875d4788055e92f9c95af757bfa7c0106))


### 🧪 Tests

* **jest:** Update globals ([de2383e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/de2383ec5bd3340485fa68bfe9372424ec99bab4))


### 🧹 Chores

* **settings:** Match latest vscode version ([cc8a328](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/cc8a3283bc8597bb84c90fbffb13b8a967637c97))

### [1.22.1](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.22.0...v1.22.1) (2023-11-30)


### 🧹 Chores

* Update node version ([8b24d01](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8b24d01507c570a94eefffc1c04c0efc8bbe6cca))


### 🐞 Bug Fixes

* **import-v2:** Import media files once ([f0a4518](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f0a4518421a11968f0c6903d4f4088c680ba1e9b))

## [1.22.0](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.21.1...v1.22.0) (2023-11-26)


### 🐞 Bug Fixes

* **types:** Fix typing ([5c86e4f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5c86e4f5bc42a68c1e0d852c4092930729c4db84))


### 🧪 Tests

* **config:** Define global strapi for ts ([de75c22](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/de75c22fe876eaae97e24b2d1063c3d2ccbc5b9f))
* **import:** Set restaurant name unique ([99f1221](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/99f1221f7c6ba53647daf909353f6641c1be08c6))
* **import:** Setup import file ([b3e14bb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b3e14bb9bfa4036570c0b0fd9536a570a87a957e))
* **import:** Test update data ([8b47c9f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8b47c9fbe81c783506b9ba30e48a329b30089a37))


### 🧹 Chores

* Setup ts jest ([fdd406b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fdd406b14bc46da9e0c71a1aadc94db1c38adfcd))
* Support node v20.x.x ([1c8f591](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1c8f5917cfac12129af1be1934736694f61d7708))

### [1.19.1](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.19.0...v1.19.1) (2023-04-25)

### 🧹 Chores

- Lint CHANGELOG ([4c5afab](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4c5afabd4f20af0506dc3233d699e820f1000489))

### 🐞 Bug Fixes

- **import-v2:** Ignore slugs with no associated schema ([c6efb54](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c6efb5461b08265173f68bc312b13dc5b9d752b9))

## [1.19.0](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.18.1...v1.19.0) (2023-04-22)

### 🧪 Tests

- **import:** Remove irrelevant tests ([ef0f97a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ef0f97a37fca494612e499977587bbb800be6ec9))
- **package.json:** Update dependencies ([fb89c4e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fb89c4efcbfd58f80392d630ceff857c080739c4))

### 🧹 Chores

- **.nvmrc:** Use lowest acceptable node version ([d1f43f1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d1f43f1b117a5b58d3c7c61bc148561fb4e9c1e7))
- Clean project before rebuild ([bdf5d8d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/bdf5d8da04403117620c1b9a9d14a2601f34c67e))
- **github:** Run CI for pull request ([b66bb6d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b66bb6d6553634ad3e6b79f83e91352c1b7380fe))
- **github:** Update CI ([099880d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/099880d04bea25f2b1fb562ff3c57b25762ebb6c))
- **jest:** Increase test timeout ([f68c45a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f68c45a7f76c1a8382d768b49408e96b69af1389))
- Lint project ([47eec1c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/47eec1c7057ead26ca2bec37db66d1eca2bc16d7))
- **package.json:** Build lib before publishing ([f30d054](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f30d054f4863fd3bcb71e8f87f3c8b477e8775d2))
- **package.json:** Update scripts ([29e3334](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/29e3334475c1466a68673110bf8a971fb16f66da))
- **parsers.js:** Delete file ([96438b1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/96438b11888ddb35a1f333066deb4e511531e0c5))
- Remove and ignore build folders ([f5b649a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f5b649a4287b60eb1f85f3b4c3b6b62929f399bf))
- Setup nodemon to hot rebuild ([982841f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/982841f49c5563de3c1eb5379442aa23f6a446a0))
- Setup typescript for server ([375a341](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/375a341aacd0a8844948f11439ad191509a8e85c))
- Switch to yarn ([4531386](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4531386c6f96b65d74279b256fb935b6da4112d6))
- **test:** Tweak config to debugging ([de4300a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/de4300ae1d730cc9374857b2236ed288b0d4b727))
- **tsconfig:** Update module import ([69cd79b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/69cd79bc8b80b97f6da7e30d9e7339e9cab21727))
- **vscode:** Recommend extensions ([d5fb201](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d5fb2016e202fb318a38f8b77b763bfa9e132bb2))

### 🐞 Bug Fixes

- **export-v2:** Export selected slugs ([a9482bd](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a9482bd24286a190df4d73f9a5f0416eb6c027dd))
- **export-v2:** Filter out ids using attribute slug ([4e6e4e7](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4e6e4e77c3aa63b223a67902ad99c3baacf99835))
- **export-v2:** Remove duplicated components in dynamic zones ([eb7ce26](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/eb7ce26c8fd10b2d418ea1e117248db479656f71))
- **gitignore:** Ignore only folders at project root ([8a172ab](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8a172ab0540e379606b8a89495534b7b4532d1b3))
- **HomePage:** Display alerts ([8b55716](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/8b557162a1f183e15b9d3fc53e346ef94986dbba))
- **import-data:** Check content permissions when import whole database ([c5c8727](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c5c8727285f12f8b086300f1807038611ee53474))
- **import-v2:** Import components and dynamic zones for not localized content types ([378a7e3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/378a7e37627c8e1451eadac28ae0b5c431308442))
- **import:** fallback to any when media field is missing allowedTypes ([0cce657](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0cce657d322ac39d737c74e0b7fc1897bab79b49))
- **package:** Only clean build folders on publish ([97e84cb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/97e84cb734ed98908e8af162a686c95d3576f0bb))
- **tests:** Comment date tests temporarily for CI ([6f5128c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6f5128c76f48592d8c974eab31e3d8cb589503fe))
- **tests:** Reset auto increment sequence ([6ca9fbb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6ca9fbb425767b090563ea798a2139a527bf7c5e))

### 🛠 Refacto

- **export-v2:** Move to ts ([69e86a5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/69e86a5ef74d862e0b7d6e8d2bb1d64e66a5da3c))
- **import-v2:** Map file ids to database ids ([e0b9383](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e0b9383c344a3328383bf8c562513041f703baa0))
- **import-v2:** Move to ts ([f492927](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f492927d2f83c08466e6f38899f20bfdf2adf051))
- **ImportEditor:** Translate component ([52b0303](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/52b0303e39057836cecf5ccd7b9615a3f1906406))
- **models:** Move to ts ([c9dadef](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c9dadef6ac4d511f3e8927b99d340ec0cecedcfb))
- **test:** Rename test files ([5e9f695](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5e9f69566028e47de09f90d2a2d88cc4c71dcc49))

### ⚡️ Features

- **get-model-attributes:** Add id field as option ([e0eabbd](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e0eabbd567cc2d09f5be4c7d12039efec018de59))
- **i18n:** Shorten translation ([abfbba3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/abfbba307847ff8fa1c2c9b0a1976e614eeaab87))
- **import-data:** Support jso format ([e4774fe](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e4774fe7b81dd796c9278b07db5f7e6afc2435cd))
- **import-v2:** Import components and dynamic zones ([d93961d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d93961dc97820c69128b82acf8e3dfba62b5185b))
- **import-v2:** Improve error understanding ([a9d576a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a9d576abd44f09cc32fd8ca16544cff77912fed4))
- **import-v2:** Support different id field per collection ([5c7da99](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5c7da999baba93ecc014deb484150c909540fd29))
- **import-v2:** Support partial import for components and dynamic zones ([40fd687](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/40fd68773fac81e2bca1920e824274bbc7bdc4fd))
- **ImportModal:** Add code editor as data source ([d4347d4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d4347d4c4bec9e576b67de72f4d8a113437acc2d))
- **README:** Explain use of idField per collection ([6222f5b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6222f5bf40acc15816fe5f6f9d87f9aa7c6e3548))
- **README:** Improve ([560d06a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/560d06a474ef2c656ce25ea59a7b88b6b79e1c77))
- **README:** Update illustration ([396b9cb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/396b9cb8e0405b00e2d2dbc997d7705ddbffb1c3))

### [1.18.1](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.18.0...v1.18.1) (2023-04-02)

### 🐞 Bug Fixes

- back to node-fetch v2 ([aa018cc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/aa018cc5028e8f36d10adcae75193a7e068557c5))
- **import:** change deprecated request by node-fetch module ([f1e63bb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f1e63bb27095e2922e7eae9395e1f2e79f6b4056))

### 🛠 Refacto

- **tests:** Move tests to specs folder ([6f0f85c](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6f0f85c73999e38bde82765b418b32789cf27876))

### 🧹 Chores

- **.versionrc.json:** Update commit keyword ([e7e35af](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e7e35af98903643463896303ba5438e4713a1f29))
- **package-lock.json:** Fix dependencies vulnerabilities ([d9bd8bc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d9bd8bc3ba429e5242169639d3b019014ca641eb))

## [1.18.0](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.17.3...v1.18.0) (2022-11-05)

### ⚡️ Features

- **admin:** Display plugin homepage ([75b2569](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/75b25698fca9e119e4616f2157787f8246e799d5))
- **export-v2:** Export whole database ([31e706a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/31e706afcf5e6c7fe9207b22572bb4acfb24a856))
- **HomePage:** Add help section ([fa08af1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fa08af17078508a374730d1c49bb9b16ee9fd36a))
- **HomePage:** Setup basic layout ([022b363](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/022b363323862a778cb89f3b5fed8abef3338158))

### 🧪 Tests

- **export-v2:** Should export whole database ([7cab210](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/7cab210f31a3b9952de4ece2746f19a50812fe05))

### 🧹 Chores

- **CHANGELOG.md:** Lint ([cc77582](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/cc775825c7aea32d3978ac86a2920be79e12196b))
- **doc:** Explain how to export whole database and setup preferences ([045c17b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/045c17be383b8b43120a7f055f79b5e9b82555a5))
- **doc:** Udpdate known limitations section ([e0e95ff](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/e0e95ff75b4c49486b0daa463668b1f521d06e14))
- **eslint:** Sort imports ([140ac0e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/140ac0e310f07c57c920e5954628e73bbd93c896))
- **husky:** Send back to APA ([efbb795](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/efbb79554b0999c24dfbf9c3ba100dba29b24b90))
- Lint project ([d4bd0a9](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/d4bd0a9d20f48dc354b6e6b2e670731ed13a3cf3))

### 🛠 Refacto

- **ExportModal:** Use preferences ([074e19f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/074e19f716aac6048980687914a431d9e13506d0))
- **useSlug:** Get model slug only on collection and single type pages ([323de8d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/323de8d5de6cdabfe70afd4f533333b68df8eb81))

### [1.17.3](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.17.2...v1.17.3) (2022-10-30)

### 🧹 Chores

- **git:** Update gitignore ([55c3145](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/55c31458e40587bec72b77469cdafaf13437cb9a))
- **husky:** Setup husky ([dcfbb35](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/dcfbb35eb53b649704f719c86436d5cbddf03277))

### 🧪 Tests

- **export-v2:** Should export components for simple single and collection types ([c1e4528](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c1e452841f68d3441caf560259e7a4b07ab53614))
- Generate unique data ([22a26e5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/22a26e5066383380e1fded3b925d66313e6fa235))
- **import-v2:** Should create or update standalone components ([97fb6b1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/97fb6b140345caa0b412f7e8f696b93731fcc349))
- **import-v2:** Should set components for simple single and collection types ([372f1e4](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/372f1e4a437ed7b0dc5965bf1c821c9b92cf644d))
- **strapi:** Cleanup components ([b86edaa](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/b86edaaee3046310acfb20ccbc96856a4c77ad78))

### 🐞 Bug Fixes

- **export-v2:** Export components for simple single and collection types ([c954791](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/c9547913b4699da87fac2f5d16345a59669600f7))
- **import-v2:** Set components for simple single and collection types ([366461f](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/366461f6c548cc808d2a0fd4823c333e58d08d99))

### [1.17.2](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.17.1...v1.17.2) (2022-10-30)

### 🧹 Chores

- **package-lock.json:** Update node engine ([1dfff19](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/1dfff19112c25a85635b1afc5b8cc171f411a89b))

### 🐞 Bug Fixes

- **import-v2:** Import collection types localized ([4a2c913](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4a2c9131a27e1c3f3e0175cc3aa25cf7751014f3))

### 🛠 Refacto

- **import:** Factorize slug in tests ([0edbccb](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0edbccb86b6d8a176dd92fa9c21c8e8b16de1a00))

### 🧪 Tests

- **collection-type:** Should create localized collection type ([efa58f5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/efa58f52f66b883d447bc7725a11b6cfa95eea83))
- **export-v2:** Should export collection type localized ([454e2d3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/454e2d345b0cadaca40e69289a8db4e762ccc6ab))
- **import-v2:** Should import collection type localized ([32f552a](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/32f552a9954e0c8c9af5778a4621b1fb4bae072c))
- **import-v2:** Should update partially single types localized ([4618509](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/4618509adda06e08f58f95741ec3fce778a5c380))

### [1.17.1](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.17.0...v1.17.1) (2022-10-28)

### ⚡️ Features

- **package.json:** Enable plugin for node 18 ([eb532cf](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/eb532cf6965b01a104ceb93169d8ea7cb86ddb24))

### 🧪 Tests

- **import-v2:** Test partial update of collection type ([3a2e8b3](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/3a2e8b393936846b13bb583e0b5d701d5a18b4fd))

### 🐞 Bug Fixes

- **import-v2:** Sync primary key sequence for postgres database ([5220b64](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/5220b6456169be6357c2e092cec13c0e4f8c310a))

### 🛠 Refacto

- **import-v2:** Find collection type entry before update ([ff6ef95](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/ff6ef95631f7cf8c9da1a03e76a54fbaebdca5cb))
- **import-v2:** Remove id field for single type ([0070693](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/007069377be471f11c629e199c34fa4ec6e2e480))

### 🧹 Chores

- **README.md:** Add section about known limitations ([2fcfeae](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/2fcfeaec58442d94caccf940d92d1b906381caaa))

### [1.17.0](https://github.com/Baboo7/strapi-plugin-import-export-entries/compare/v1.16.2...v1.17.0) (2022-10-27)

### 🧹 Chores

- **package:** Add standard version ([24739b1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/24739b11af04019ec9684db0805c80f4c56f08fd))
- **test:** Load strapi plugins ([f43609b](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/f43609bac660134e621d9cf1b461dfa5552024df))

### ⚡️ Features

- **ExportModal:** Mark csv export as deprecated ([fbc64db](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/fbc64dbc4f691c1ca81e067b1b1c810cfefa2cb3))
- **test:** Setup database before tests ([931566d](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/931566d7ce2d329601afd5a8870a141d16744826))

### 🐞 Bug Fixes

- **export-v2:** Export single type localized ([09dc3cc](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/09dc3ccaac27921c6c5f2c9fc822886784c3eaf3))
- **import-v2:** Import single type ([35b54c5](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/35b54c5d4ec730f9159059589a0ba1c522b36565))

### 🧪 Tests

- **export-v2:** Test export localizations of single type localized ([da8c434](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/da8c4344d104705e7eed5e3d226855eab57f0b8f))
- **export-v2:** Test export of single type localized ([0fd1b9e](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/0fd1b9e9c39f93d1fda62cf7c3a96f60d8dd5b03))
- **import-v2:** Test import of single type ([6092b67](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/6092b67c6088d86e660a6194bee679c9323be0bb))
- **import:** Comment limitation about single type localized ([a5576c1](https://github.com/Baboo7/strapi-plugin-import-export-entries/commits/a5576c1897cb1435e31ad35ceff949b994e7e225))
