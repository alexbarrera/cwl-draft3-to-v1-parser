from __future__ import print_function

import argparse
import re
import textwrap

import errno
import ruamel.yaml as ryaml
import sys
from collections import OrderedDict
import copy
import os
import shutil

LASTEST_VERSION = 'v1.0'


def update_clt(t):
    def update_type(tt):
        if 'type' in tt and tt['type'] == 'array':
            return update_type(tt['items']) + '[]'
        return tt

    if 'description' in t:
        t['doc'] = t['description']
        del t['description']

    for in_out in ['inputs', 'outputs']:
        aux = {}
        for inp in t[in_out]:
            if len(inp['type']) == 2:  # For more than 2 types, leave it
                if 'null' in inp['type'] and  type(inp['type'][0]) == str and type(inp['type'][1]) == str:
                    inp['type'].remove('null')
                    inp['type'] = str(inp['type'][0]) + '?'
                else:
                    inp['type'] = update_type(inp['type'])
            if 'description' in inp:
                inp['doc'] = inp['description']
                del inp['description']
            val = inp['id'].lstrip('#').split('.')[-1]
            del inp['id']
            aux[val] = inp
        t[in_out] = aux

    aux = {}
    if 'hints' in t:
        for inp in t['hints']:
            val = inp['class']
            del inp['class']
            aux[val] = inp
        t['hints'] = aux

    aux = {}
    if 'requirements' in t:
        for inp in t['requirements']:
            val = inp['class']
            del inp['class']
            aux[val] = inp
        t['requirements'] = aux

    t['cwlVersion'] = LASTEST_VERSION
    return t


def update_workflow(t, cwlctx='root'):
    if type(t) == list:

        if 'null' in t:  # replace 'null' for '?'
            if len(t) == 2:
                del t[t.index('null')]
                return "%s?" % update_workflow(t[0], cwlctx=cwlctx)
            else:
                return [update_workflow(a, cwlctx=cwlctx) for a in t]  # If multiple arguments, as in draft-3

        for i, e in enumerate(t):
            if type(e) == dict and 'id' in e:
                val = e['id'].lstrip('#').split('.')[-1]
                if (cwlctx == 'instep') and 'source' in e: # Change id for output value
                    del e['id']
                    s = e['source'].lstrip('#').replace('.', '/')
                    if 'valueFrom' not in e and 'secondaryFiles' not in e:
                        del e['source']
                        t[i] = update_workflow(e)
                        t[i].update({val: s})
                    else:
                        e['source'] = s
                        t[i] = update_workflow(e, cwlctx=cwlctx)
                        t[i] = {val: t[i]}
                elif (cwlctx == 'outputs') and 'source' in e:  # Change source in outputs for outputSource
                    s = e['source'].lstrip('#').replace('.', '/')
                    del e['id']
                    del e['source']
                    t[i] = update_workflow(e, cwlctx=cwlctx)
                    t[i].update({"outputSource": s})
                    t[i] = {val: t[i]}
                elif cwlctx == 'outstep':
                    t[i] = val
                else:
                    del e['id']
                    t[i] = {val: update_workflow(e, cwlctx=cwlctx)}
            else:
                t[i] = update_workflow(e, cwlctx=cwlctx)

    if type(t) == dict:
        if cwlctx == 'type':
            if 'type' in t and t['type'] == 'array':  # replace 'type: array' for '[]'
                del t['type']
                return "%s[]" % update_workflow(t['items'], cwlctx='type') # Use items as value
            else:
                # print "ERROR! type object does not have type field %s" % repr(t)
                pass

        for k, v in t.iteritems():
            if k == 'cwlVersion':
                t[k] = LASTEST_VERSION
            elif k == 'run':
                if type(v) == str:
                    t[k] = v
                else:
                    t[k] = v.values()[0]
            elif k == 'description':
                del t[k]
                t['doc'] = v.replace("\n", " ")
            elif k == 'steps':
                steps={}
                for i, e in enumerate(update_workflow(v, cwlctx='steps')):
                    for kk, vv in e.iteritems():
                        steps[kk] = vv
                t[k] = steps
            elif cwlctx == 'instep' and k == 'source':
                t[k] = v.lstrip("#").replace('.', '/')

            elif cwlctx == 'steps':
                if k == 'outputs':
                    del t[k]
                    t['out'] = update_workflow(v, cwlctx='outstep')
                elif k == 'scatter':
                    if type(v) == str:
                        t[k] = v.split('.')[-1]
                    else:
                        t[k] = [vv.split('.')[-1] for vv in v]

                elif k == 'inputs':
                    del t[k]
                    insteps = {}
                    for i, e in enumerate(update_workflow(v, cwlctx='instep')):
                        for kk, vv in e.iteritems():
                            insteps[kk] = vv
                    t['in'] = insteps
                else:
                    t[k] = update_workflow(v, cwlctx=cwlctx)
            elif k == 'outputs':
                outs = {}
                for i, e in enumerate(update_workflow(v, cwlctx='outputs')):
                    for kk, vv in e.iteritems():
                        outs[kk] = vv
                t[k] = outs
            elif k == 'inputs':
                ins = {}
                for i, e in enumerate(update_workflow(v, cwlctx=cwlctx)):
                    for kk, vv in e.iteritems():
                        ins[kk] = vv
                t[k] = ins
            elif k == 'type':
                v_orig = copy.deepcopy(v)
                t[k] = update_workflow(v, cwlctx='type')
                if type(t[k]) == str and '?' in t[k] and not t[k].endswith('?'):  # If nested optionals, as in draft-3
                    t[k] = v_orig
            else:
                t[k] = update_workflow(v, cwlctx=cwlctx)
    elif type(t) == str:
        t = t.rstrip()
    return t


class WorkflowYaml(object):
    def __init__(self, target_file):
        try:
            self.target = ryaml.load(open(target_file), Loader=ryaml.Loader)
        except TypeError:
            self.target = ryaml.load(target_file, Loader=ryaml.Loader)
            target_file.seek(0, 0)

    def load_target_as_ruamel_obj(self, target_file):
        try:
            self.target = ryaml.load(open(target_file), Loader=ryaml.RoundTripLoader)
        except TypeError:
            self.target = ryaml.load(target_file, Loader=ryaml.RoundTripLoader)


def print_parsed_obj(cwlyml, target_path=None, ofd=sys.stdout):
    if cwlyml.target['class'] == 'Workflow':
        cwlyml.target = update_workflow(cwlyml.target)
    else:
        cwlyml.load_target_as_ruamel_obj(target_path)
        cwlyml.target = update_clt(cwlyml.target)

    root_keys = ['class',
                 'cwlVersion',
                 'doc',
                 'requirements',
                 'hints',
                 'inputs',
                 'steps',
                 'expression',
                 'outputs',
                 'baseCommand',
                 'arguments',
                 'stdin',
                 'stdout']

    # print(ryaml.round_trip_dump(cwlyml.target, indent=2, explicit_end=False))
    st = ryaml.round_trip_dump(OrderedDict(sorted(cwlyml.target.items(),
                                                  key=lambda t: root_keys.index(t[0]))),
                               indent=2,
                               explicit_end=False,
                               width=10000)
    final = re.sub('^-', '', re.sub('\n\n', '\n', st)).replace('!!omap\n-', '')
    ofd.write(re.sub('\n-', '\n', final))
    #

def copytree(src, dst):
    try:
        shutil.copytree(src, dst)
    except OSError as exc:  # python >2.5
        if exc.errno == errno.ENOTDIR:
            pass
        if exc.errno == errno.EEXIST:  # Static folder exists
            shutil.rmtree(dst)
            copytree(src, dst)
        else:
            raise

from ruamel.yaml.scalarstring import PreservedScalarString, preserve_literal


def walk_tree(base):
    from ruamel.yaml.compat import string_types

    def test_wrap(v):
        v = v.replace('\r\n', '\n').replace('\r', '\n').strip()
        return v if len(v) < 72 else preserve_literal(v)

    if isinstance(base, dict):
        for k in base:
            v = base[k]
            if isinstance(v, string_types) and '\n' in v:
                base[k] = test_wrap(v)
            else:
                walk_tree(v)
    elif isinstance(base, list):
        for idx, elem in enumerate(base):
            if isinstance(elem, string_types) and '\n' in elem:
                base[idx] = test_wrap(elem)
            else:
                walk_tree(elem)


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=textwrap.dedent('''
            Transform CWL files from version draft-3 to v1.0
            ------------------------------------------------
            '''))

    parser.add_argument('target', type=str, metavar='workflow-draft-3.cwl', help='File to be transformed')
    parser.add_argument('--outdir', metavar='path/to/outdir',
                        help='Path to the directory where the input file tree structure will be copied')
    parser.add_argument('--cwl-extension', type=str, default='cwl',
                        help='Extension used to look up for CWL files when a whole directory tree is processed')

    args = parser.parse_args()

    target_path = args.target

    if os.path.isdir(target_path):
        if not args.outdir:
            print("ERROR::Missing outdir argument")
            sys.exit(1)

        copytree(target_path, args.outdir)

        for (dirpath, dirnames, filenames) in os.walk(target_path):
            for filename in filenames:
                if filename.endswith('.' + args.cwl_extension):
                    filepath = os.sep.join([dirpath, filename])
                    newfilepath = os.sep.join(
                        [re.sub('^%s' % target_path, args.outdir, dirpath), filename]
                    )
                    with open(newfilepath, 'w') as ofile:
                        cwlyml = WorkflowYaml(filepath)
                        print_parsed_obj(cwlyml, target_path=filepath, ofd=ofile)
    else:
        cwlyml = WorkflowYaml(target_path)
        print_parsed_obj(cwlyml, target_path=target_path)


if __name__ == '__main__':
    main()