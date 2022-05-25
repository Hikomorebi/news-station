import json
import os
from datetime import datetime

import networkx as nx
from networkx import NetworkXError
from networkx.utils import not_implemented_for

from indexbuilder.IdMap import IdMap

def query2date(str,date_format="%Y-%m-%d %H:%M:%S"):
    date = datetime.datetime.strptime(str, date_format)
    return date
if not os.path.isfile('./news.json'):
            f = open('./news.json', 'wb')
            f.close()
idmap = IdMap()
G = nx.Graph() # new graph
with open('../nkuspider/data.json', 'r', encoding='utf-8') as f:
    for line in f.readlines():
        doc = line.strip('\n').strip(',')
        docj = json.loads(doc)

        id1 = idmap._get_id(docj['newsUrl'])


        out = docj['out_degree']
        length = len(out)
        for i in out:
            id2 = idmap._get_id(i)
            G.add_edge(id1, id2, weight=100/length)
f.close()


@not_implemented_for('multigraph')
def pagerank(G, alpha=0.85, personalization=None,
             max_iter=100, tol=1.0e-6, nstart=None, weight='weight',
             dangling=None):
    """Return the PageRank of the nodes in the graph.
    Parameters
    -----------
    G : graph
        A NetworkX graph. 在PageRank算法里面是有向图
    alpha : float, optional
        稳定系数, 默认0.85, 心灵漂移teleporting系数，用于解决spider trap问题
    personalization: dict, optional
      个性化向量，确定在分配中各个节点的权重
      格式举例，比如四个点的情况: {1:0.25,2:0.25,3:0.25,4:0.25}
      默认个点权重相等，也可以给某个节点多分配些权重，需保证权重和为1.
    max_iter : integer, optional
        最大迭代次数
    tol : float, optional
        迭代阈值
    nstart : dictionary, optional
        整个网络各节点PageRank初始值
    weight : key, optional
      各边权重
    dangling: dict, optional
      字典存储的是dangling边的信息
      key   --dangling边的尾节点，也就是dangling node节点
      value --dangling边的权重
      PR值按多大程度将资源分配给dangling node是根据personalization向量分配的
      This must be selected to result in an irreducible transition
      matrix (see notes under google_matrix). It may be common to have the
      dangling dict to be the same as the personalization dict.
    Notes
    -----
    特征值计算是通过迭代方法进行的，不能保证收敛，当超过最大迭代次数时，还不能减小到阈值内，就会报错
    """

    # 步骤一：图结构的准备--------------------------------------------------------------------------------
    if len(G) == 0:
        return {}

    if not G.is_directed():
        D = G.to_directed()
    else:
        D = G

    # Create a copy in (right) stochastic form
    W = nx.stochastic_graph(D, weight=weight)
    N = W.number_of_nodes()

    # 确定PR向量的初值
    if nstart is None:
        x = dict.fromkeys(W, 1.0 / N)  # 和为1
    else:
        # Normalized nstart vector
        s = float(sum(nstart.values()))
        x = dict((k, v / s) for k, v in nstart.items())

    if personalization is None:
        # Assign uniform personalization vector if not given
        p = dict.fromkeys(W, 1.0 / N)
    else:
        missing = set(G) - set(personalization)
        if missing:
            raise NetworkXError('Personalization dictionary '
                                'must have a value for every node. '
                                'Missing nodes %s' % missing)
        s = float(sum(personalization.values()))
        p = dict((k, v / s) for k, v in personalization.items())  # 归一化处理

    if dangling is None:
        # Use personalization vector if dangling vector not specified
        dangling_weights = p
    else:
        missing = set(G) - set(dangling)
        if missing:
            raise NetworkXError('Dangling node dictionary '
                                'must have a value for every node. '
                                'Missing nodes %s' % missing)
        s = float(sum(dangling.values()))
        dangling_weights = dict((k, v / s) for k, v in dangling.items())

    dangling_nodes = [n for n in W if W.out_degree(n, weight=weight) == 0.0]

    # dangling_nodes  dangling节点
    # danglesum       dangling节点PR总值

    # dangling初始化  默认为personalization
    # dangling_weights  根据dangling而生成，决定dangling node资源如何分配给全局的矩阵

    # 迭代计算--------------------------------------------------------------------

    # PR=alpha*(A*PR+dangling分配)+(1-alpha)*平均分配
    # 也就是三部分，A*PR其实是我们用图矩阵分配的，dangling分配则是对dangling node的PR值进行分配，(1-alpha)分配则是天下为公大家一人一份分配的

    # 其实通俗的来说，我们可以将PageRank看成抢夺大赛，有三种抢夺机制。
    # 1，A*PR这种是自由分配，大家都愿意参与竞争交流的分配
    # 2，dangling是强制分配，有点类似打倒土豪分田地的感觉，你不参与自由市场，那好，我们就特地帮你强制分。
    # 3，平均分配，其实就是有个机会大家实现共产主义了，不让spider trap这种产生rank sink的节点捞太多油水，其实客观上也是在帮dangling分配。

    # 从图和矩阵的角度来说，可以这样理解，我们这个矩阵可以看出是个有向图
    # 矩阵要收敛-->矩阵有唯一解-->n阶方阵对应有向图是强连通的-->两个节点相互可达，1能到2,2能到1
    # 如果是个强连通图，就是我们上面说的第1种情况，自由竞争，那么我们可以确定是收敛的
    # 不然就会有spider trap造成rank sink问题

    for _ in range(max_iter):
        xlast = x
        x = dict.fromkeys(xlast.keys(), 0)  # x初值
        danglesum = alpha * sum(xlast[n] for n in dangling_nodes)  # 第2部分：计算dangling_nodes的PR总值
        for n in x:
            for nbr in W[n]:
                x[nbr] += alpha * xlast[n] * W[n][nbr][weight]  # 第1部分:将节点n的PR资源分配给各个节点，循环之
        for n in x:
            x[n] += danglesum * dangling_weights[n] + (1.0 - alpha) * p[n]  # 第3部分：节点n加上dangling nodes和均分的值

        # 迭代检查
        err = sum([abs(x[n] - xlast[n]) for n in x])
        if err < N * tol:
            return x
    raise NetworkXError('pagerank: power iteration failed to converge '
                        'in %d iterations.' % max_iter)
x = pagerank(G) #计算每个文档的pagerank值
print(x)
with open('../nkuspider/data.json', 'r', encoding='utf-8') as f:
    with open('./news.json','w', encoding='utf-8') as d:
        for line in f.readlines():
            doc = line.strip('\n').strip(',')
            docj = json.loads(doc)
            del docj['newsUrlMd5']
            del docj['indexed']
            del docj['out_degree']
            sd = docj['newsPublishTime']
            sd = sd[5:].strip()
            docj['newsPublishTime'] = sd
            id1 = idmap._get_id(docj['newsUrl'])
            print(id1)
            docj['pagerank'] = x[id1]
            print(docj)
            line = json.dumps(docj, ensure_ascii=False) + "\n"
            d.write(line)
