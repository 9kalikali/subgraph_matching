
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <queue>
#include <vector>
#include <set>
#include <bitset>
#include <map>
#include <iostream>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) SubGraphMatch##name

using namespace std;

//---------------------------------A struct TreeNode for Query Tree------------------------------------//
class TreeNode{
public:
    int id;     // 查询图顶点id, 不唯一
    int du;     // 节点的度,作为临时使用的变量，在使用之后已经并非是节点的度
    int vecid;  // 在树中的唯一标识，相当于treeVec[i] 中的i（id并不是唯一标识，这才是唯一标识）
    int value;  // 查询图顶点值
    int edge_value; // 查询图边值
    TreeNode* parent; // 算法中有频繁访问父节点的操作，所以保存一个该节点的父节点指针
    vector<TreeNode *> children; // 该节点的孩子节点

    TreeNode(){}

    TreeNode(int id, int val)
    {
        this->id = id;
        this->value = val;
        this->parent = NULL;
    }

    void set(int id, int val, TreeNode* parent)
    {
        this->id = id;
        this->value = val;
        this->parent = parent;
    }

    // find node
    // param1: sid  要查询的树节点vecid
    // param2: root 要查询的树根节点
    static TreeNode* getNodeById(int sid, TreeNode& root)
    {
        TreeNode* result = NULL;
        queue<TreeNode *> search_queue;
        search_queue.push(&root);
        while( !search_queue.empty() )
        {
            if(sid == search_queue.front()->vecid)
            {
                result = search_queue.front();
                break;
            }
            else
            {
                for(TreeNode* node : search_queue.front()->children)
                {
                    search_queue.push(node);
                }
                search_queue.pop();
            }

        }
        return result;
    }
    // find leaves
    // param1: root 要查询的树根节点
    static vector<TreeNode *> getLeaves(TreeNode& root)
    {
        vector<TreeNode *> leaves;
        queue<TreeNode *> search_queue;
        search_queue.push(&root);
        while( !search_queue.empty() )
        {
            if(search_queue.front()->children.size() == 0)
            {
                leaves.push_back(search_queue.front());
                search_queue.pop();
            }
            else
            {
                for(TreeNode* node : search_queue.front()->children)
                {
                    search_queue.push(node);
                }
                search_queue.pop();
            }
        }
        return leaves;
    }

};

map <pair<int ,int> , int > edgeMap1;//存边值,以点的id为判断条件

struct SMapPoint{
    int pointValue;
    vector< int > edgeKey;
};

struct edge{
    //并非单向边 记录信息
    int from,to,value;

};



vector<SMapPoint> SMapPVec;
vector<edge> SMapEVec;
map<int ,int> useEdge;
vector<TreeNode> treeVec1;//中间过程的树
vector<TreeNode> treeVec2;//MHT，其中TreeVec2[0]为根节点




//---------------------------------Values used in the algorithm--------------------------------------//
#define MAX_LEN 5000  // max length of bitset
static int SEG = 52; // equals to size of Data Graph
static int tree_height = 0; // height of query tree, set up in Graph::init()
vector<set<int> > Res;  //to store query result
typedef bitset<MAX_LEN> TreeCache;

void searchG()
{
    FILE *fp1;
    //文件输入
    //
    fp1=fopen("/home/bdms/setup/GraphLite-0.20/Input/Query_1.txt","r");
    //fp1=fopen("/home/bdms/setup/GraphLite-0.20/bin/SMaps.txt","r");
    
    int pointNum,edgeNum,color1,color2,from2,to2;

    //输入数据 第一行点数n边数m 然后n行点+m行边
    fscanf(fp1,"%d%d",&pointNum,&edgeNum);
    for(int i=0;i<pointNum;i++){
        //建点的第一部分

        fscanf(fp1,"%d",&color1);
        SMapPoint p1;
        p1.pointValue = color1;

        SMapPVec.push_back( p1  );

    }

    /*for(int i=0;i<SMapPVec.size();i++){

        printf("%d %d\n",i,SMapPVec[i].pointValue);
    }*/

    for(int i=0;i<edgeNum;i++){
        //建边+建点的第二部分

        fscanf(fp1,"%d%d%d",&from2,&to2,&color2);
        edge p2;
        p2.from = from2;
        p2.to = to2;
        p2.value = color2;
        pair <int ,int> plin1(from2,to2),plin2(to2,from2);
        edgeMap1[plin1] = color2;
        edgeMap1[plin2] = color2;

        SMapEVec.push_back( p2 );
        SMapPVec[from2].edgeKey.push_back(i);
        SMapPVec[to2].edgeKey.push_back(i);

    }
    map<int ,int > useEdge;//保证每个边只遍历一次，遍历后打上标记

    for(auto edge : SMapEVec)
    {
        cout << edge.value << endl;
    }

    //先以0节点建立树。



    treeVec1.reserve(5000);//固定最多5000节点的树，可改
    treeVec2.reserve(5000);

    TreeNode node1;

    node1.set(0,SMapPVec[0].pointValue,&node1);

    treeVec1.push_back(node1);

    //开始建中间树,注:该树为无向图.



    int sta=0;

    while (sta<treeVec1.size()){


        int pKey = treeVec1[sta].id;
        //printf("hello %d %d\n",pKey,SMapPVec[pKey].edgeKey.size());

        for(int i = 0 ;i<SMapPVec[pKey].edgeKey.size();i++){

           // printf("233\n");

            int edKey = SMapPVec[pKey].edgeKey[i];

            if(useEdge[edKey]==1) continue;
            useEdge[edKey]=1;

            //printf("--%d\n",edKey);

            if(SMapEVec[edKey].from == pKey){
                //printf("---%d\n",edKey);

                TreeNode linNode;
                linNode.set(SMapEVec[edKey].to,SMapPVec[ SMapEVec[edKey].to ].pointValue,&treeVec1[sta]);



               // printf("**%d %d\n",linNode.id,linNode.parent->id);

                treeVec1.push_back(linNode);
                treeVec1[sta].children.push_back(&treeVec1[treeVec1.size()-1 ]   );
                //linNode.vecid=treeVec1.size();

               // printf("**%d %d\n",linNode.id,linNode.parent->id);

            }
            else{
               // printf("----%d\n",edKey);

                TreeNode linNode;
                linNode.set(SMapEVec[edKey].from,SMapPVec[ SMapEVec[edKey].from ].pointValue,&treeVec1[sta]);
                treeVec1.push_back(linNode);
                treeVec1[sta].children.push_back(&treeVec1[treeVec1.size()-1 ]);

                //linNode.vecid=treeVec1.size();

                //printf("**%d %d\n",linNode.id,linNode.parent->id);
            }
        }
        sta+=1;

    }
    treeVec1[0].parent=&treeVec1[0];
    for(int i=0;i<treeVec1.size();i++){

        treeVec1[i].vecid = i;

        /*TreeNode linNode;
        linNode = treeVec1[i];

        printf("id:%d parent:%d\n",linNode.id,linNode.parent->vecid);*/
    }




    //开始用剥洋葱法找真正的图心
    
    // for(int i=0;i<treeVec1.size();i++){
    //     pair<int ,int> Plin3(treeVec1[i].id,treeVec1[i].parent->id);
    //     treeVec1[i].edge_value=edgeMap1[Plin3];

    //     printf("id:%d parent:%d parentedg:%d  childrenNum:%d\n",treeVec1[i].vecid,treeVec1[i].parent->id,treeVec1[i].edge_value,treeVec1[i].children.size());

    //     for(int k=0;k<treeVec1[i].children.size();k++){
    //         printf("      %d    \n",treeVec1[ treeVec1[i].children[k]->vecid ].vecid);
    //     }
    // }


    int ans1=0;
    tree_height=0;

    for(int i=0;i<treeVec1.size();i++){

        int lindu;
        lindu=1;
        if(treeVec1[i].id==treeVec1[i].parent->id){
            lindu=0;
        }
        lindu = lindu + treeVec1[i].children.size();
        treeVec1[i].du = lindu;


    }


    /*for(int i=0;i<treeVec1.size();i++){

        TreeNode linNode;
        linNode = treeVec1[i];

       // printf("id:%d vecId:%d childNum:%d\n",linNode.id,linNode.vecid,linNode.children.size());
        for(int k=0;k<linNode.children.size();k++){
            pair<int ,int> plin(linNode.id,linNode.children[k]->id);

            printf("%d %d  value:%d\n",linNode.vecid,linNode.children[k]->vecid,edgeMap1[plin] );
        }
    }*/
    /*for(int i=0;i<treeVec1.size();i++){

        TreeNode linNode;
        linNode = treeVec1[i];

        printf("id:%d du:%d\n",linNode.id,linNode.du);

    }*/
    //从度为1的点开始剥洋葱(byc),每次去掉度为1的点，并更新其他的度，最终胜<2个的时候即可
    int sum = treeVec1.size();

    while(1){
        //只剩两个点以内，则随便
        tree_height++;

        if(sum<=2){
            if(sum==2) tree_height++;
            for(int i=0;i<treeVec1.size();i++){
                if(treeVec1[i].du>0){
                    ans1 = i;
                    break;
                }
            }
            break;
        }

        //减去度为1的点后若只剩两个，则同上
        int lin=0;
        for(int i=0;i<treeVec1.size();i++){
            if(treeVec1[i].du==1){
                lin++;
                treeVec1[i].du=0;
            }
        }
        if(sum-lin<=2){
            tree_height++;
            if(sum-lin==2) tree_height++;
            for(int i=0;i<treeVec1.size();i++){
                if(treeVec1[i].du>0){
                    ans1 = i;

                    break;
                }
            }
            break;
        }

        sum=sum-lin;
        //printf("%d\n",sum);
        //更新度
        //printf("du0::::%d  du1::::::%d\n",treeVec1[0].du,treeVec1[1].du);


        for(int i=0;i<treeVec1.size();i++){
            if(treeVec1[i].du==0) continue;
            int lin2=0;
            //printf("%d\n",treeVec1[i].children.size());
            for(int k = 0; k < treeVec1[i].children.size();k++){
                int kp = treeVec1[i].children[k]->vecid;
                //printf("kp:%d\n",kp);
                if(treeVec1[kp].du>0){
                    lin2++;
                }
            }
           // printf("%d\n",i);
            if(i!=0){
                int kp2 = treeVec1[i].parent->vecid;
                if(treeVec1[kp2].du>0){
                    lin2++;
                }
            }
            treeVec1[i].du = lin2;
        }
        //printf("du0::::%d  du1::::::%d\n",treeVec1[0].du,treeVec1[1].du);



    }


    //printf("ans1 = %d id = %d\n",ans1,treeVec1[ans1].id);

    //以treeVec1[ans1]为根节点建立MHT

    sta=0;
    treeVec2.push_back(treeVec1[ans1]);
    treeVec2[0].children.clear();
    useEdge.clear();

    while (sta<treeVec2.size()){


        int pKey = treeVec2[sta].id;
        //printf("hello %d %d\n",pKey,SMapPVec[pKey].edgeKey.size());

        for(int i = 0 ;i<SMapPVec[pKey].edgeKey.size();i++){

            //printf("233\n");

            int edKey = SMapPVec[pKey].edgeKey[i];

            if(useEdge[edKey]==1) continue;
            useEdge[edKey]=1;

            //printf("--%d\n",edKey);

            if(SMapEVec[edKey].from == pKey){
                //printf("---%d\n",edKey);

                TreeNode linNode;
                linNode.set(SMapEVec[edKey].to,SMapPVec[ SMapEVec[edKey].to ].pointValue,&treeVec2[sta]);

               // printf("**%d %d\n",linNode.id,linNode.parent->id);

                treeVec2.push_back(linNode);
                treeVec2[sta].children.push_back(&treeVec2[treeVec2.size()-1 ]   );
                //linNode.vecid=treeVec1.size();

                //printf("**%d %d\n",linNode.id,linNode.parent->id);

            }
            else{
               // printf("----%d\n",edKey);

                TreeNode linNode;
                linNode.set(SMapEVec[edKey].from,SMapPVec[ SMapEVec[edKey].from ].pointValue,&treeVec2[sta]);
                treeVec2.push_back(linNode);
                treeVec2[sta].children.push_back(&treeVec2[treeVec2.size()-1 ]);

                //linNode.vecid=treeVec1.size();

                //printf("**%d %d\n",linNode.id,linNode.parent->id);
            }
        }
        sta+=1;

    }
    treeVec2[0].parent=&treeVec2[0];
    for(int i=0;i<treeVec2.size();i++){

        treeVec2[i].vecid = i;

        TreeNode linNode;
        linNode = treeVec2[i];

        //printf("id:%d parent:%d\n",linNode.id,linNode.parent->id);
    }
    //建立父亲边
    for(int i=0;i<treeVec2.size();i++){
        pair<int ,int> Plin3(treeVec2[i].id,treeVec2[i].parent->id);
        treeVec2[i].edge_value=edgeMap1[Plin3];

       // printf("id:%d edge_value:%d\n",treeVec2[i].id,treeVec2[i].edge_value);
    }

}

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(int);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(int);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(pair<int, int>);
        return m_m_value_size;
    }
    void loadGraph() {
        if (m_total_edge <= 0)  return;

        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        int weight;
        int out_value;
        int in_value;
        int outdegree = 0;
        bool flag = true;
        
        const char *line= getEdgeLine();

        //Read-in format: from_id to_id from_value edge_value

        sscanf(line, "%lld %lld %d %d", &from, &to, &out_value, &weight);
        addEdge(from, to, &weight);
        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line= getEdgeLine();

            int in_value;
            sscanf(line, "%lld %lld %d %d", &from, &to, &in_value, &weight);
            if (last_vertex != from) {
                if(flag)
                {
                    addVertex(last_vertex, &out_value, outdegree);
                    flag = false;
                }
                else
                {
                    addVertex(last_vertex, &in_value, outdegree);
                }
                
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &in_value, outdegree);

    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        double value;
        char s[1024];

        for(auto Mp : Res)
        {
            int n = sprintf(s, "Query Result: \n");
            writeNextResLine(s, n);
            for(auto node : Mp)
            {
                int n = sprintf(s, "vid: %d \n", node);
                writeNextResLine(s, n);
            }
        }
    }
};

class VERTEX_CLASS_NAME(Aggregator): public Aggregator< TreeCache > {
public:
    void init() {
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        m_global =  *(TreeCache *)p;
    }
    void* getLocal() {
        return &m_local;
    }
    void merge(const void* p) {
        TreeCache local = *(TreeCache *) p;
        m_global |= local;
    }
    void accumulate(const void* p) {

        TreeCache acc = *(TreeCache *) p;
        m_local |= acc;
    }
};

class VERTEX_CLASS_NAME(): public Vertex <int, int, pair<int, int> > {
public:
    set<int> Mp;
    vector<int> counter;    // to judge if this vertex recieved all subtrees of parent node

    void compute(MessageIterator* pmsgs) {
        if (getSuperstep() == 0) 
        {   
            // Debug log:
            cout << "vid : " << getVertexId() << " value : " << getValue() << endl;
            for(TreeNode* leaf : treeVec2[0].getLeaves(treeVec2[0]))
            {
                // if leaf l matches v
                if(leaf->value == getValue())
                {
                    pair<int, int> matchnodes(leaf->vecid, getVertexId());
                    // put pair into treecache
                    TreeCache omega;
                    int offset = matchnodes.first * SEG;
                    omega.set(offset + matchnodes.second);
                    accumulateAggr(0, &omega);
                    // sendMessage(u, m)
                    sendMessage(leaf, matchnodes);
                }
            }
        }
        if( getSuperstep() > 0 && getSuperstep() < tree_height) 
        {
            TreeCache subtrees = *(TreeCache *)getAggrGlobal(0);
            // Debug log:
            cout << "subtrees  " << subtrees.count() << endl;
            // vector<TreeNode *> temp_parent;
            
            for(; !pmsgs->done(); pmsgs->next())
            {

                pair<int, int> matchnodes = pmsgs->getValue();
                TreeNode* node  = treeVec2[0].getNodeById(matchnodes.first, treeVec2[0]);
                // if u's parent u' matches v
                if( node->parent->value == getValue())
                {
                    // record node parent
                    // temp_parent.push_back(node->parent);
                    // record subtree root id
                    counter.push_back(matchnodes.first);

                    // Debug log:
                     cout << "subroot : " << matchnodes.first << " in vertex " << getVertexId() << endl;

                    // Mp <- Mp U {Omega(u)}, Mp is a set which can gurantee every element is unique.
                    for(int i = (matchnodes.first * SEG); i < (matchnodes.first * SEG + SEG); i++)
                    {
                        if(subtrees[i]) Mp.insert(i % SEG);
                    }

                    // Still not certain how to implement 'v recieves all Omega(u_i) and u_i is Child(u')' 
                    if(counter.size() >= node-ca>parent->children.size())
                    {   
                        pair<int, int> new_matchnodes(node->parent->vecid, getVertexId());
                        Mp.insert(getVertexId());
                        TreeCache omega;
                        int offset = new_matchnodes.first * SEG;
                        for(auto vid : Mp)
                        {
                            omega.set(offset + vid);
                        }
                        accumulateAggr(0, &omega);
                        // if reached root
                        if( node->parent->vecid == treeVec2[0].vecid )
                        {                           
                            Res.push_back(Mp);
                        }
                        else
                        {
                            sendMessage(node->parent, new_matchnodes);
                        }
                    }
                }
            }
            voteToHalt();
            return;
        }
    }

    // implementation of sendMessage(u, m)
    void sendMessage(TreeNode* node, pair<int, int>& msg)
    {
        // sendMessageToAllNeighbors(msg);
        OutEdgeIterator it = getOutEdgeIterator();
        for(; ! it.done(); it.next())
        {
            int dst = it.target();
            // Note: it.getValue() can obtain the value of out edge
            cout << "Node " << node->vecid << " edge :" << node->edge_value << " Vertex " << getVertexId() << " edge: " << it.getValue() << endl;
            if(node->edge_value == it.getValue())
            {
                sendMessageTo(dst, msg);
                cout << "send to " << dst << endl;
            }
        }
    }

};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;
public:
    // argv[0]: SubGraphMatch.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    void init(int argc, char* argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 3) {
           printf ("Usage: %s <input path> <output path>\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
        
        //-------------Read Query Graph Here and generate Query Tree-------------//

        searchG();
        cout << "tree height " << tree_height << endl;

    }

    void term() {
        delete[] aggregator;
    }

};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
