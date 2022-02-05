{
  path ? "/../aws-list.txt",
}:

with import ./common.nix;
let 
  pkgs = import pkgsSrc {};
  lib = pkgs.lib;

  /* Converts a key list and a value list to a set

     Example:
       listToSet [ "name" "version" ] [ "latex" "3.14" ]
       => { name = "latex"; version = "3.14"; }
  */
  listToSet = keys: values: 
    builtins.listToAttrs 
      (lib.zipListsWith 
        (a: b: { name = a; value = b; }) 
        keys
        values);

  /* Says if datetime a is more recent than datetime b

    Example:
      cmpDate { date = "2021-09-10"; time = "22:12:15"; } { date = "2021-02-03"; time = "23:54:12"; }
      => true
  */
  cmpDate = a: b: 
    let da = (builtins.head a.builds).date;
        db = (builtins.head b.builds).date;
    in 
      if da == db then  (builtins.head a.builds).time > (builtins.head b.builds).time
      else da > db;

  /* Pretty platforms */
  prettyPlatform = name:
    if name == "aarch64-unknown-linux-musl" then "linux/arm64"
    else if name == "armv6l-unknown-linux-musleabihf" then "linux/arm"
    else if name == "x86_64-unknown-linux-musl" then "linux/amd64"
    else if name == "i686-unknown-linux-musl" then "linux/386"
    else name;

  /* Parsing */
  list = builtins.readFile (./. + path);
  entries = lib.splitString "\n" list;

  elems = builtins.filter 
    (e: (builtins.length e) == 4) 
    (map 
      (x: builtins.filter (e: e != "") (lib.splitString " " x)) 
      entries);

  keys = ["date" "time" "size" "path"];
  parsed = map (entry: listToSet keys entry) elems;

  subkeys = ["root" "version" "platform" "binary" ];
  builds = map (entry: entry // listToSet subkeys (lib.splitString "/" entry.path) // { url = "https://garagehq.deuxfleurs.fr/" + entry.path; }) parsed;

  /* Aggregation */
  builds_per_version = lib.foldl (acc: v: acc // { ${v.version} = if builtins.hasAttr v.version acc then acc.${v.version} ++ [ v ] else [ v ]; }) {} builds; 

  versions = builtins.attrNames builds_per_version;
  versions_release = builtins.filter (x: builtins.match "v[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?" x != null) versions;
  versions_commit = builtins.filter (x: builtins.match "[0-9a-f]{40}" x != null) versions;
  versions_extra = lib.subtractLists (versions_release ++ versions_commit) versions;

  sorted_builds = [
    {
      name = "Release";
      hide = false;
      type = "tag";
      description = "Release builds are the official builds, they are tailored for productions and are the most tested.";
      builds =  builtins.sort (a: b: a.version > b.version) (map (x: { version = x; builds = builtins.getAttr x builds_per_version; }) versions_release);
    }
    {
      name = "Extra";
      hide = true;
      type = "tag";
      description = "Extra builds are built on demand to test a specific feature or a specific need.";
      builds = builtins.sort cmpDate (map (x: { version = x; builds = builtins.getAttr x builds_per_version; }) versions_extra);
    }
    {
      name = "Development";
      hide = true;
      type = "commit";
      description = "Development builds are built periodically. Use them if you want to test a specific feature that is not yet released.";
      builds = builtins.sort cmpDate (map (x: { version = x; builds = builtins.getAttr x builds_per_version; }) versions_commit);
    }
  ];
in
{
  json = pkgs.writeTextDir "share/_releases.json" (builtins.toJSON sorted_builds);

  html = pkgs.writeTextDir "share/_releases.html" ''
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Garage releases</title>
    <style>
      html, body { margin:0; padding: 0 }
      body { font-family: 'Helvetica', Sans; }
      section { margin: 1rem; }
      ul { padding:0; margin: 0.2rem }
      li {
        border-radius: 0.2rem;
        display: inline; 
        border: 2px #0b5d83 solid; 
        padding: 0.5rem; 
        line-height: 3rem;
        color: #0b5d83;
      }
      li:hover { background-color: #0b5d83; color: #fff; }
      li a, li a:hover { color: inherit; text-decoration: none }
    </style>
  </head>
  <body>
   ${ builtins.toString (lib.forEach sorted_builds (r: ''
    <section>
      <h2>${r.name} builds</h2>

      <p>${r.description}</p>

      ${if r.hide then "<details><summary>Show ${r.name} builds</summary>" else ""}
      ${ builtins.toString (lib.forEach r.builds (x: ''
        <h3> ${x.version} (${(builtins.head x.builds).date}) </h3>
        <p>See this build on</p>
        <p> Binaries:
        <ul>
        ${ builtins.toString (lib.forEach x.builds (b: ''
          <li><a href="/${b.path}">${prettyPlatform b.platform}</a></li>
        ''))}
        </ul></p>
        <p> Sources:
        <ul>
         <li><a href="https://git.deuxfleurs.fr/Deuxfleurs/garage/src/${r.type}/${x.version}">gitea</a></li>
         <li><a href="https://git.deuxfleurs.fr/Deuxfleurs/garage/archive/${x.version}.zip">.zip</a></li>
         <li><a href="https://git.deuxfleurs.fr/Deuxfleurs/garage/archive/${x.version}.tar.gz">.tar.gz</a></li>
        </ul></p>
      ''))  }
      ${ if builtins.length r.builds == 0 then "<em>There is no build for this category</em>" else "" }
      ${if r.hide then "</details>" else ""}
    </section>
  ''))}
  </body>
</html>
'';
}
