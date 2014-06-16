#!/usr/bin/env python

# Tool which synchonises a given document between two clusters, including metadata.
# Given references to two clusters, if the document is *only* found on one
# of the two it is copied to the other.

# If it exists on both clusters, then it will refuse to copy them.

from __future__ import print_function

import memcacheConstants
from mc_bin_client import MemcachedClient
from mc_bin_client import MemcachedError

import sys
import optparse


def get_matching_meta(cluster, key, attempts):
    """Attempt to get a matching value and metadata for a key (using get() and
    getMeta(). Will retry up to attempts times. Returns a tuple of the fields
    on success, else None."""
    for _ in range(attempts):
            (deleted, flags, exp, seqno, meta_cas) = cluster.getMeta(key)
            if deleted:
                value = ""
                break
            (_, cas, value) = cluster.get(key)
            if cas == meta_cas:
                break
    else:
        # Failed
        return None
    return (deleted, flags, exp, seqno, meta_cas, value)


def print_doc(title, deleted, flags, exp, seqno, cas, value):
    if deleted:
        value = "DELETED"
    print(("  {0:20} : deleted:{1} flags:{2} exp:{3} seqNo:{4} CAS:{5} " +
           "value:{6}...").format(title, deleted, flags, exp, seqno, cas,
           value[:30]))

def synchronize_key(src, dest, key):
    """Reads a document+metadata from the source; then attempts to set the same
    doc+meta on the destination."""

    global options
    print("Key: {}".format(key))

    try:
        result = get_matching_meta(src, key, 3)
    except MemcachedError as e:
        if e.status == memcacheConstants.ERR_KEY_ENOENT:
            print("  Error: no such key '{}' on souce - skipping.".format(key))
            return
        else:
            raise

    if not result:
        print(("  Error: failed to get consistant data & " + 
               "metadata from source - skipping.").format(key))
        return
    (s_deleted, s_flags, s_exp, s_seqno, s_cas, s_value) = result

    if options.verbose:
        print_doc("Source", s_deleted, s_flags, s_exp, s_seqno, s_cas,
                  s_value)

    result = None
    try:
        result = get_matching_meta(dest, key, 3)
    except MemcachedError as e:
        if e.status != memcacheConstants.ERR_KEY_ENOENT:
            raise

    if result:
        (d_deleted, d_flags, d_exp, d_seqno, d_cas, d_value) = result
        if options.verbose:
            print_doc("Dest before sync", d_deleted, d_flags, d_exp, d_seqno,
                      d_cas, d_value)

        if (s_deleted, s_flags, s_exp, s_seqno, s_cas, s_value) == (d_deleted, d_flags, d_exp, d_seqno, d_cas, d_value):
            print("  Source and Destination match exactly - skipping.")
            return

    if result and options.force:
        # Check revIDs are increasing.
        if d_seqno > s_seqno:
            if options.allow_src_changes:
                # We are allowed to change source, so fix this by bumping
                # up the source's to dest_revID+1.
                src.setWithMeta(key, s_value, s_exp, s_flags, d_seqno + 1,
                                s_cas, s_cas)
                # Refetch CAS, etc from new document.
                result = get_matching_meta(src, key, 3)
                if not result:
                    print(("  Error: failed to get consistant data & " +
                           "metadata from source - skipping.").format(key))
                    return
                (s_deleted, s_flags, s_exp, s_seqno, s_cas, s_value) = result
                if options.verbose:
                    print_doc("Source after revID fix", s_deleted, s_flags,
                              s_exp, s_seqno, s_cas, s_value)

            else:
                print(("Error: Destination revID '{}' greater than source " +
                       "revID '{}'. Cannot synchronize unless " +
                       "--allow-source-changes is enabled.").format(d_seqno,
                                                                    s_seqno))
                return
        try:
            dest.setWithMeta(key, s_value, s_exp, s_flags, s_seqno, s_cas,
                             d_cas)

        except MemcachedError as e:
            if e.status == memcacheConstants.ERR_KEY_EEXISTS:
                print("Error: Got EEXISTS during setWithMeta(). Possible " +
                      "CAS mismatch setting at destination.")
    else:
        try:
            dest.addWithMeta(key, s_value, s_exp, s_flags, s_seqno, s_cas)

        except MemcachedError as e:
            if e.status == memcacheConstants.ERR_KEY_EEXISTS:
                print(("Error: key '{}' already exists on destination " + 
                       "cluster. Run with --force to overwrite.").format(key))
            else:
                raise

    # Fetch to double-check it matches:
    result = get_matching_meta(dest, key, 3)
    if not result:
        print(("Error: failed to get consistant data & metadata from " +
               "destination after set.").format(key))
        return
    (d_deleted, d_flags, d_exp, d_seqno, d_cas, d_value) = result

    same = ((s_deleted, s_flags, s_exp, s_seqno, s_cas, s_value) == (d_deleted, d_flags, d_exp, d_seqno, d_cas, d_value))
    if same:
        print("  OK")
    else:
        print("ERROR: Src & dest differ *after* setWithMeta:")

    if not same or options.verbose:
        print_doc("Dest after sync", d_deleted, d_flags, d_exp, d_seqno, d_cas,
                  d_value)


def main(args):
    parser = optparse.OptionParser()
    parser.add_option('-s','--source-bucket', dest="src_bucket", default="default",
                      help="source bucket to use")
    parser.add_option('-d','--dest-bucket', dest="dest_bucket", default="default",
                      help="destination bucket to use")
    parser.add_option('-f', '--force', action='store_true', dest='force',
                      help='Overwrite destination document if it already exists.')
    parser.add_option('-a', '--allow-source-changes', action='store_true', dest='allow_src_changes',
                      help=('Allow changes to the source metadata ' +
                            '(e.g. revID) to be made if necessary to synchronize documents.'))
    parser.add_option('-v', '--verbose', action='store_true', dest='verbose',
                      help='Verbose')

    global options
    options, args = parser.parse_args()

    password = ""

    if len(args) < 3:
        print("Usage: sync-doc <src_cluster> <dest_cluster> <keys..>")
        exit(1)

    src_port = dest_port = 11211
    src_name = args.pop(0)
    dest_name = args.pop(0)
    if ':' in src_name:
        (src_name, src_port) = src_name.split(':')
    if ':' in dest_name:
        (dest_name, dest_port) = dest_name.split(':')

    src = MemcachedClient(src_name, int(src_port))
    dest = MemcachedClient(dest_name, int(dest_port))
    src.sasl_auth_plain(options.src_bucket, password)
    dest.sasl_auth_plain(options.dest_bucket, password)

    for key in args:
        synchronize_key(src, dest, key)

if __name__ == '__main__':
    sys.exit(main(sys.argv))
