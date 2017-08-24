# coding: utf8
import os
import logging
import sys
import re

from collections import defaultdict
from statflow.mr.localstreamer import LocalStreamer
from subprocess import PIPE, Popen
from lazy import lazy

logger = logging.getLogger(__name__)


class ChainNode(object):

    def __init__(self, src, node_type, name, dst=None, step=None, files=None, original_step_number=None):
        self.step = step
        if not isinstance(src, list):
            raise Exception('Source should be list')
        self.src = src
        self.dst = dst
        self.node_type = node_type
        self.original_step_number = original_step_number
        self.files = files
        self.name = name


class ChainPlan(object):

    def __init__(self, nodes, context, tmp_prefix):
        self.nodes = nodes
        self.context = context
        self.tmp_prefix = tmp_prefix

    @lazy
    def publisher_nodes(self):
        result = []
        for node in self.nodes:
            try:
                if node.step.publication() is not None:
                    result.append(node)
            except AttributeError:
                continue
        return result

    @property
    def src(self):
        result = set()
        for node in self.nodes:
            if node.node_type != 'mr':
                continue
            for src in node.src:
                if src.startswith(self.tmp_prefix):
                    continue
                result.add(src)
        return list(result)

    @property
    def dst(self):
        result = set()
        for node in self.nodes:
            if node.node_type != 'mr':
                continue
            if node.dst.startswith(self.tmp_prefix):
                continue
            result.add(node.dst)
        return list(result)


class Chain(object):

    steps = []
    is_adhoc = False
    _template_regex = re.compile(r'\{\{\s*([^\s\}]+)\s*\}\}')

    @classmethod
    def _apply_template(cls, text, params):
        def _subsitute(matchobj):
            return params[matchobj.group(1)]
        return cls._template_regex.sub(_subsitute, text)

    @classmethod
    def _plan_mr_steps(cls, context, tmp, path_prefix):
        """
        This function generate a sequance of map reduce nodes with resolved dst and src paths using self.steps list.
        """
        node_list = []
        template_params = {
            'prev': '<bad marker>',
            'next': None,
            'date': context['execution_date'].date().isoformat(),
            'tmp': tmp
        }
        for step_num, step in enumerate(cls.steps):
            template_params['next'] = os.path.join(tmp, step.__name__, 'auto-' + str(step_num))
            template_params['step'] = step.__name__
            template_params['chain'] = cls.__name__
            src_list = []
            for v in step.context_src(context):
                src = cls._apply_template(v, template_params)
                if '<bad marker>' in src:
                    raise Exception('Bad src for step %s step_num %s.' % (step.__name__, step_num))
                if not src.startswith('/'):
                    src = os.path.join(path_prefix, src)
                src_list.append(src)
            dst = cls._apply_template(step.context_dst(context), template_params)
            if not dst.startswith('/'):
                dst = os.path.join(path_prefix, dst)
            files = []
            for v in step.context_files(context):
                file_src = cls._apply_template(v, template_params)
                if '<bad marker>' in file_src:
                    raise Exception('Bad file src for step %s step_num %s.' % (step.__name__, step_num))
                if not file_src.startswith('/'):
                    file_src = os.path.join(path_prefix, file_src)
                files.append(file_src)
            node_list.append(ChainNode(src_list, 'mr', cls.__name__ + step.__name__, dst=dst, step=step, files=files, original_step_number=step_num))
            template_params['prev'] = dst
        return node_list

    @classmethod
    def _plan_garbage_collection(cls, node_list, tmp):
        new_node_list = []
        reference_counter = defaultdict(lambda: 0)
        for node in node_list:
            for src in node.src + map(lambda f: f.split('#')[0], node.files):
                if src.startswith(tmp):
                    reference_counter[src] += 1
        for node in node_list:
            new_node_list.append(node)
            src_to_delete = set()
            for src in node.src + map(lambda f: f.split('#')[0], node.files):
                if src.startswith(tmp):
                    reference_counter[src] -= 1
                    if reference_counter[src] == 0:
                        src_to_delete.add(src)
            if len(src_to_delete) > 0:
                new_node_list.append(ChainNode(list(src_to_delete), 'DeleteTempTable', cls.__name__ + node.step.__name__ + 'DeleteTempTable'))
        return new_node_list

    @classmethod
    def plan(cls, context, path_prefix):
        tmp = os.path.join(
            path_prefix,
            'tmp',
            context['execution_date'].date().isoformat(),
            cls.__name__
        )
        node_list = cls._plan_mr_steps(context, tmp, path_prefix)
        node_list = cls._plan_garbage_collection(node_list, tmp)
        return ChainPlan(node_list, context, tmp)


class LocalChainRunner(object):
    # TODO move it in a separate file

    def __init__(self, chain_plan):
        self.chain_plan = chain_plan

    def _filter_sources(self, sources, path_prefix):
        node_sources = [os.path.join(path_prefix, src.strip('/')) for src in sources]
        node_sources_files = []
        for node_source in node_sources:
            if not os.path.exists(node_source):
                continue
            if os.path.isfile(node_source):
                node_sources_files.append(node_source)
                continue
            for f in os.listdir(node_source):
                full_file_path = os.path.join(node_source, f)
                if os.path.isfile(full_file_path):
                    node_sources_files.append(full_file_path)
        return node_sources_files

    def run_mr_step(self, node, path_prefix):
        from shutil import copyfile, rmtree
        from statflow.config import config
        from tempfile import mkdtemp

        logger.info('src before filter %s', node.src)
        sources = self._filter_sources(node.src, path_prefix)
        logger.info('src after filter %s', sources)
        if not sources:
            raise Exception('Src can not be empty')

        postmapdata = os.path.join(path_prefix, os.path.dirname(node.dst), 'postmapdata')
        if not os.path.exists(os.path.dirname(postmapdata)):
            os.makedirs(os.path.dirname(postmapdata))
        src = sources if not node.step.has_map else [postmapdata]
        dst = os.path.join(path_prefix, node.dst.lstrip('/')) if not node.step.has_reduce else postmapdata
        cls_args = {'date': self.chain_plan.context['execution_date'].date().isoformat()}

        file_src = [f.split('#')[0] for f in node.files]
        file_names = [f.split('#')[1] for f in node.files]
        files = self._filter_sources(file_src, path_prefix)

        if len(file_src) != len(files):
            raise Exception('You have to ensure the existence of all files that described in your Step - `%s`' % files)
        temp_path = mkdtemp(dir=config.statflow.tmp.path())
        try:
            os.chdir(temp_path)
            for f, name in zip(files, file_names):
                name = name if name != '' else os.path.basename(f)
                copyfile(f, name)

            if node.step.has_map:
                LocalStreamer.run(node.step, 'map', sources, dst, cls_args)
            if node.step.has_reduce:
                f = open(os.path.join(path_prefix, 'sorted_reduce_source'), 'w')
                p = Popen('cat %s' % ' '.join(src), shell=True, stdout=PIPE)
                Popen('sort', shell=True, stdin=p.stdout, stdout=f, env={'LC_ALL': 'C'}).communicate()
                p.stdout.close()
                f.close()
                LocalStreamer.run(node.step, 'reduce', [f.name], os.path.join(path_prefix, node.dst.lstrip('/')), cls_args)
        finally:
            rmtree(temp_path, ignore_errors=True)

    def run_garbage_collection(self, node):
        for src in node.src:
            os.remove(src)

    def run_chain(self, path_prefix, start_step=0, finish_step=sys.maxint):
        for node in self.chain_plan.nodes:
            if node.original_step_number is not None and not finish_step >= node.original_step_number >= start_step:
                continue
            if node.node_type == 'DeleteTempTable':
                logger.info('Start garbage collection %s', node.src)
                # self.run_cleaner(node)
            elif node.node_type == 'mr':
                logger.info('Start mr step %s', node.step.__name__)
                self.run_mr_step(node, path_prefix)
                logger.info('Finish mr step. dst %s', os.path.join(path_prefix, node.dst.lstrip('/')))
