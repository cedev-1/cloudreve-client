// The Foundation calls real Mac apps make on a document, run against a
// file on the mounted drive by `tests/mounted_macos_apps.rs`:
//
//   safe-save <file> <text>  how NSDocument apps (TextEdit, Preview, Pages)
//                            save: write a temp copy in the "item
//                            replacement" folder Foundation picks for that
//                            volume, then swap it in with replaceItemAt.
//   trash <file>             what Finder's "Move to Trash" (Cmd+Delete) asks
//                            Foundation to do.
//   tag <file> <tag>         set a Finder tag, then read the tags back.
//
// Prints one line: "ok ..." or "error <domain> <code> <description>".
import Foundation

let args = CommandLine.arguments
func fail(_ error: Error) -> Never {
    let ns = error as NSError
    print("error \(ns.domain) \(ns.code) \(ns.localizedDescription)")
    exit(0)
}

guard args.count >= 3 else { print("usage"); exit(2) }
let fm = FileManager.default
let target = URL(fileURLWithPath: args[2])

switch args[1] {
case "safe-save":
    do {
        let scratch = try fm.url(for: .itemReplacementDirectory, in: .userDomainMask,
                                 appropriateFor: target, create: true)
        let temp = scratch.appendingPathComponent(target.lastPathComponent)
        try args[3].data(using: .utf8)!.write(to: temp)
        _ = try fm.replaceItemAt(target, withItemAt: temp, backupItemName: nil, options: [])
        try? fm.removeItem(at: scratch)
        print("ok scratch=\(scratch.path)")
    } catch { fail(error) }
case "trash":
    do {
        var resulting: NSURL?
        try fm.trashItem(at: target, resultingItemURL: &resulting)
        print("ok trashed=\(resulting?.path ?? "")")
    } catch { fail(error) }
case "tag":
    do {
        var url = target
        var values = URLResourceValues()
        values.tagNames = [args[3]]
        try url.setResourceValues(values)
        url.removeAllCachedResourceValues()
        let back = try url.resourceValues(forKeys: [.tagNamesKey]).tagNames ?? []
        print("ok tags=\(back.joined(separator: ","))")
    } catch { fail(error) }
default:
    print("usage"); exit(2)
}
