/**
 * @author: zhangyuwei
 */

#include "ns3/log.h"
#include "control-algorithm.h"
#include <algorithm>

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("ControlAlgorithm");

NS_OBJECT_ENSURE_REGISTERED(ControlAlgorithm);

TypeId ControlAlgorithm::GetTypeId(void) {
  static TypeId tid = TypeId("ns3::ControlAlgorithm")
    .SetParent<Object>()
    .SetGroupName("OpenFlow")
  ;
  return tid;
}

ControlAlgorithm::ControlAlgorithm() {
  NS_LOG_FUNCTION(this);
  m_period = 0.2; 
}

ControlAlgorithm::ControlAlgorithm(Ptr<Topology> topo)
{
  NS_LOG_FUNCTION(this);
  m_topo = topo;
  m_period = 0.2;
}

ControlAlgorithm::~ControlAlgorithm() {
  NS_LOG_FUNCTION(this);
}

void ControlAlgorithm::SetTopology(Ptr<Topology> topo) {
  NS_LOG_FUNCTION(this);
  m_topo = topo;
}

void ControlAlgorithm::SetTrafficMatrix(TrafficMatrixs matrix) {
  NS_LOG_FUNCTION(this);
}

void ControlAlgorithm::SetMessageBus(Ptr<MessageBus> mb) {
  NS_LOG_FUNCTION(this);
  m_mb = mb;
}

void ControlAlgorithm::SetStorage(Ptr<Storage> storage) {
  NS_LOG_FUNCTION(this);
  m_storage = storage;
}

void ControlAlgorithm::SetSimuTime(double time) {
  NS_LOG_FUNCTION(this);
  m_simuTime = time;
}

void ControlAlgorithm::SetPeriod(double period) {
  NS_LOG_FUNCTION (this);
  m_period = period;
}

void ControlAlgorithm::Start (void) {
  NS_LOG_FUNCTION (this);

  Simulator::Schedule(Seconds(m_period), &ControlAlgorithm::CalculateNewPaths, this);
  //Simulator::Schedule(Seconds(m_period), &ControlAlgorithm::ChangeWFQWeight,this);
}

void ControlAlgorithm::SetDefaultPaths() {
  NS_LOG_FUNCTION(this);

  // set flow entry for host <-> controller
  for (int i = 0; i < m_topo->m_numHost; ++i) {
    if (i != m_topo->m_ctrlPos) {
      NS_LOG_INFO("Host <--> Controller");
      NS_LOG_INFO("ReRoute h-c " << i << " --> " << m_topo->m_ctrlPos);
      Path_t path = m_topo->Dijkstra(i, m_topo->m_ctrlPos);
      m_topo->SetDefaultFlowEntry(path);
    }
  }
  
  // set flow entry for switch <--> controller
  for (int i = 0; i < m_topo->m_numSw; ++i) {
    NS_LOG_INFO("Switch <--> Controller");
    NS_LOG_INFO("ReRoute s-c " << m_topo->m_numHost+i << " --> " << m_topo->m_ctrlPos);
    Path_t path = m_topo->Dijkstra(m_topo->m_numHost+i, m_topo->m_ctrlPos);
    m_topo->SetDefaultFlowEntry(path);
  }

  // calculate default path from host <--> host
  for (int i = 0; i < m_topo->m_numHost; ++i) {
    for (int j = i + 1; j < m_topo->m_numHost; ++j) {
      if (i != m_topo->m_ctrlPos && j != m_topo->m_ctrlPos) {
        for (int tos = 0; tos < MAXTOS; ++tos) {
          NS_LOG_INFO("Host <--> Host");

          // i --> j
          //NS_LOG_INFO(" Current ToS is: " << tos << " Path: " << m_storage->m_allPaths[i][j][4][0]);
          Path_t path = m_storage->m_allPaths[i][j][tos%K];
          NS_LOG_INFO("ReRoute h-h path " << i << " --> " << j << ":" << tos);
          for (int k = 0; k < (int)path.size(); ++k) {
            NS_LOG_INFO(path[k]);
          }
          m_storage->m_paths[tos][i][j] = path;
          m_topo->SetDefaultPath(path, tos);

          if (tos == 0) {
            m_topo->SetDefaultPath(path, 255);
          }

          // reverse path j --> i
          path = m_storage->m_allPaths[j][i][tos%K];
          NS_LOG_INFO("ReRoute h-h path " << j << " --> " << i << ":" << tos);
          for (int k = 0; k < (int)path.size(); ++k) {
            NS_LOG_INFO(path[k]);
          }
          m_storage->m_paths[tos][j][i] = path;
          m_topo->SetDefaultPath(path, tos);

          if (tos == 0) {
            m_topo->SetDefaultPath(path, 255);
          }
        }
      }
    }
  }
}

// sort multiple congesion link by compare of utilization
bool utilizationCmp(const LinkStatus &lhs, const LinkStatus &rhs) {
  return lhs.utilization > rhs.utilization;
}

// sort multiple flow by compare of stream demand
bool demandCmp(const StreamDemandInfo &lhs, const StreamDemandInfo &rhs) {
  return lhs.demand < rhs.demand;
}

void ControlAlgorithm::DetermineIdealRate()
{
  NS_LOG_FUNCTION(this);

  if ((int)m_linkstat.size() != m_topo->m_numEdge) {
    m_linkstat.resize(m_topo->m_numEdge);
  }
  
  // init links utilization
  for (int i = 0; i < (int)m_linkstat.size(); ++i) {
    m_linkstat[i].idx = i;
    m_linkstat[i].utilization = 0;
    m_linkstat[i].totalTime = 0;
    if ( i < (m_topo->m_numSw/2) * (m_topo->m_numSw/2) || (i >= (m_topo->m_numSw/2) * (m_topo->m_numSw/2) + m_topo->m_numHost && i < m_topo->m_numHost + 2 * (m_topo->m_numSw/2) * (m_topo->m_numSw/2)) )
      m_linkstat[i].bandwidth = 0.1 * LINKBANDWIDTH;
    else 
      m_linkstat[i].bandwidth = 1 * LINKBANDWIDTH;
    //NS_LOG_INFO("DetermineIdealRate module, the " << i << "-th link, bandwidth: " << m_linkstat[i].bandwidth);
  }

  /*//When the per-flow max-min is enabled, plese uncomment the following codes:
  for (int tos = 0; tos < MAXTOS; ++tos){
    for (int src = 0; src < m_topo->m_numHost; ++src){
      for (int dst = 0; dst < m_topo->m_numHost; ++dst){
        if (m_storage->m_tm[tos][src][dst].flowCounts!=0) {
	m_storage->m_tm[tos][src][dst].ratedFlag = false;
        if (m_storage->m_tm[tos][src][dst].flowCounts!=0 && m_storage->m_tm[tos][src][dst].routedFlag == false) {
          //init coflow weights:
	  std::map<int,int>::iterator it;
          it = apps_Weight.find(tos);
          if (it == apps_Weight.end()) {
            apps_Weight.insert( std::pair<int,int>(tos,1) );  
          NS_LOG_INFO("coflow weight,tos: " << tos << " weight: " << apps_Weight.find(tos)->second);
          }
         }
         }
       }
     }
   }*/
    

  // When the per-flow max-min is enabled, please comments the following codes:
  for (int tos = 0; tos < MAXTOS; ++tos){
    bool flag = false; // represents whether the coflow has at least one flow;

    // Record demands of all flows from the application:
    std::vector<StreamDemandInfo> demandVec;
    std::vector<double> bottleneckCounts(m_linkstat.size(),0.0); 
    for (int src = 0; src < m_topo->m_numHost; ++src){
      for (int dst = 0; dst < m_topo->m_numHost; ++dst){
        //Yuanxiang Gao adds:
        //init ratedFlag:
        if (m_storage->m_tm[tos][src][dst].flowCounts!=0) {
	m_storage->m_tm[tos][src][dst].ratedFlag = false;
        }
        if (m_storage->m_tm[tos][src][dst].flowCounts!=0 && m_storage->m_tm[tos][src][dst].routedFlag == false) {
        flag = true;
        
        //init coflow weights:
	std::map<int,int>::iterator it;
        it = apps_Weight.find(tos);
        if (it == apps_Weight.end()) 
          apps_Weight.insert( std::pair<int,int>(tos,1) );  
        NS_LOG_INFO("coflow weight,tos: " << tos << " weight: " << apps_Weight.find(tos)->second);

        /*//For strategy-proof experiments, please uncomment following codes:
        if(tos == 8 || tos == 7 || tos == 2 || tos == 17 || tos == 11 || tos == 13) {
          m_storage->m_tm[tos][src][dst].streamSize /= 100;
        }*/
	
         NS_LOG_INFO("determineIdealRate, tos: " << tos << " src: " << src << " dst: " << dst << " flowCounts*streamSize " << (int)m_storage->m_tm[tos][src][dst].flowCounts*m_storage->m_tm[tos][src][dst].streamSize);
         StreamDemandInfo demandInfo;
         demandInfo.src = src;
         demandInfo.dstHost = dst;
         demandInfo.tos = tos;
         demandInfo.demand = m_storage->m_tm[tos][src][dst].flowCounts*m_storage->m_tm[tos][src][dst].streamSize;
         demandVec.push_back(demandInfo);      
        }
       }
     }
  if (flag==true) {     

    //Determining IdealRate:
    if ((int)demandVec.size()!=0) {
    for (int j = 0; j < (int)demandVec.size(); ++j) {
    	//int tos = demandVec[j].tos;
    	int src = demandVec[j].src;
    	int dst = demandVec[j].dstHost;
    	double demand = (double)demandVec[j].demand;
        NS_LOG_INFO("Route for single coflow module, src: " << src << " dst: " << dst << " tos: " << tos << " flowCounts*streamSize " << demand);

    	std::vector<Path_t> alterPaths = m_storage->m_allPaths[src][dst];
    	int size = alterPaths.size();        

    	int p = 0;

        /*//Heuristic-based load balancing scheme for a single coflow:
    	double tempBottleneck;
    	tempBottleneck=10000;
    	for (int k = 0; k < size; ++k) {
     	  Path_t alterPath = alterPaths[k];
          for (int l=0; l<(int)alterPath.size(); ++l) {
	    NS_LOG_INFO("The " << l << "-th link of the path is: " << alterPath[l]);
          }
          double maxBottleneck=0;
      	  for (int l = 0; l < (int)alterPath.size(); l++) {
            if(bottleneckCounts[alterPath[l]]+demand/m_linkstat[alterPath[l]].bandwidth>maxBottleneck)
              maxBottleneck=bottleneckCounts[alterPath[l]]+demand/m_linkstat[alterPath[l]].bandwidth;
          }
      	  if (maxBottleneck<tempBottleneck) {
            tempBottleneck=maxBottleneck;
            p = k;
          }
          NS_LOG_INFO("The maximal bottleneck time along the path is: " << maxBottleneck);
        }*/
    

        //ECMP based random routing for a single coflow:
        //We enforce the ECMP find a path with exact 4 links:
        do {
          p = rand()%size;
        }while (alterPaths[p].size()!=4 && alterPaths[p].size()!=2);
 
    ////update link status along the selected path:
    for (int l = 0; l < (int)alterPaths[p].size(); ++l)
      {
         NS_LOG_INFO(" The selected path is: " << alterPaths[p][l]);
         bottleneckCounts[alterPaths[p][l]] += demand/m_linkstat[alterPaths[p][l]].bandwidth;
      }
    }
    }
  //Determining ideal rate for flows of the coflow:
  for (int l=0; l<(int)bottleneckCounts.size(); ++l)
  {
    NS_LOG_INFO("The bottleneck time on " << l << "-th link is: " << (int)bottleneckCounts[l]);
  }
  double idealInvRate = 0;
  for (uint32_t l = 0; l < bottleneckCounts.size(); ++l)
    if (bottleneckCounts[l] > idealInvRate)
      idealInvRate = bottleneckCounts[l];
 
  NS_LOG_INFO(" The inverse ideal rate is: " << idealInvRate); 

  //record the ideal CCT of the coflow:
  //apps_idealCCT.insert( std::pair<double,int>(idealInvRate,tos));

  //if performance-centric fairness is enabled, please uncomment following codes:
  //idealInvRate = 1; 

  //NS_LOG_INFO("ideal CCT, tos: " << tos << " ideal CCT: " << apps_idealCCT.find(idealInvRate)->first); 

  for (int src = 0; src < m_topo->m_numHost; ++src){
      for (int dst = 0; dst < m_topo->m_numHost; ++dst){
          if (m_storage->m_tm[tos][src][dst].flowCounts!=0) {
		m_storage->m_tm[tos][src][dst].idealRate = 1/idealInvRate*m_storage->m_tm[tos][src][dst].streamSize;
                NS_LOG_INFO(" The ideal rate for src: " << src << " dst: " << dst << " is: " << m_storage->m_tm[tos][src][dst].idealRate);
          }
      }
  }

  }
 }
}

void ControlAlgorithm::UpdateLinksUtilization() {
  NS_LOG_FUNCTION(this);

  for (int tos = 0; tos < MAXTOS; ++tos){
    for (int src = 0; src < m_topo->m_numHost; ++src){
      for (int dst = 0; dst < m_topo->m_numHost; ++dst){
        if (m_storage->m_tm[tos][src][dst].flowCounts!=0 && (m_storage->m_tm[tos][src][dst].routedFlag == true))
        {
          Path_t path = m_storage->m_paths[tos][src][dst];
          for (int i = 0; i < (int)path.size(); ++i) {
            //coflow max-min:
            m_linkstat[path[i]].utilization += m_storage->m_tm[tos][src][dst].idealRate*apps_Weight.find(tos)->second;
            m_linkstat[path[i]].totalTime += m_storage->m_tm[tos][src][dst].streamSize*apps_Weight.find(tos)->second;             
   
            //per-flow max-min:
            //m_linkstat[path[i]].utilization += m_storage->m_tm[tos][src][dst].flowCounts*apps_Weight.find(tos)->second;

            //NS_LOG_INFO("in updatelinkutilization module, src: " << src << " dst: " << dst << " tos: " << tos << " through link: " << path[i] << " utilization: " << m_linkstat[path[i]].utilization);
            //NS_LOG_INFO("in updatelinkutilization module, src: " << src << " dst: " << dst << " tos: " << tos << " through link: " << path[i] << " totalTime: " << m_linkstat[path[i]].totalTime);
          }
        }
      }
    }
  }

  for (int i = 0; i < (int)m_linkstat.size(); ++i) {
    m_linkstat[i].utilization /= m_linkstat[i].bandwidth;
    m_linkstat[i].totalTime /= m_linkstat[i].bandwidth;
    if (m_linkstat[i].utilization > 0) {
      NS_LOG_INFO("update link utilization module link: " << i << " utilization: " << m_linkstat[i].utilization << " totalTime: " << m_linkstat[i].totalTime);
    }
  }
}

bool ControlAlgorithm::CalculateNewPaths() {
  NS_LOG_FUNCTION(this);
  std::cout << "!!!At time " << Simulator::Now().GetSeconds() << "s ControlAlgorithm CALCULATENEWPATHS Start" << std::endl;


  //Determine the ideal rate for each coflow:
  DetermineIdealRate();

  // Update current link utilization
  UpdateLinksUtilization();

   // record demands of all un-rerouted flows:
    std::vector<StreamDemandInfo> demandVec;
 
    for (int tos = 0; tos < MAXTOS; ++tos) 
      for (int src = 0; src < m_topo->m_numHost; ++src) {
        for (int dst = 0; dst < m_topo->m_numHost; ++dst) { 
          if (m_storage->m_tm[tos][src][dst].flowCounts != 0 && m_storage->m_tm[tos][src][dst].routedFlag == false)
          { 
            m_storage->m_tm[tos][src][dst].routedFlag = true;
            // Get traffic demand from Matrix
	    StreamDemandInfo demandInfo;
	    demandInfo.src = src;
	    demandInfo.dstHost = dst;
	    demandInfo.tos = tos;

            //coflow max-min fairness:
            demandInfo.demand = m_storage->m_tm[tos][src][dst].idealRate*apps_Weight.find(tos)->second;
            demandInfo.streamSize = m_storage->m_tm[tos][src][dst].streamSize*apps_Weight.find(tos)->second;

            //per-flow max-min fairness:
            //demandInfo.demand = m_storage->m_tm[tos][src][dst].flowCounts*apps_Weight.find(tos)->second;

	    demandVec.push_back(demandInfo);
           }
        }
      } 
   ReRouteAllDemands(demandVec);  

  /*//Smallest-idealCCT-first rate allocation (Yuanxiang Gao adds):
  //rate allocation logic: 
  for (std::map<double, int>::iterator iter = apps_idealCCT.begin(); iter!= apps_idealCCT.end(); ++iter) {
    int tos = iter->second;
    NS_LOG_INFO("in SJF rate allocation, tos: " << (int)tos << " idealCCT " << iter->first);
    for (int src =0; src < m_topo->m_numHost; ++src) {
      for (int dst = 0; dst < m_topo->m_numHost; ++dst) { 
        if (m_storage->m_tm[tos][src][dst].flowCounts != 0 && m_storage->m_tm[tos][src][dst].ratedFlag == false)
	  { 
            NS_LOG_INFO("Smallest-idealCCT-first: One unrated flow is, src: " << src << " dst: " << dst);
            double allocatedRate = 10000;
            for (int i = 0; i < (int)m_storage->m_paths[tos][src][dst].size(); ++i) {
	      if (m_linkstat[m_storage->m_paths[tos][src][dst][i]].bandwidth < allocatedRate)
               {
                 allocatedRate = m_linkstat[m_storage->m_paths[tos][src][dst][i]].bandwidth;
               }
             }

             m_storage->m_tm[tos][src][dst].allocatedRate = allocatedRate;
             m_storage->m_tm[tos][src][dst].ratedFlag = true;
             for (int l = 0; l < (int)m_storage->m_paths[tos][src][dst].size(); ++l) {
		m_linkstat[m_storage->m_paths[tos][src][dst][l]].bandwidth -= m_storage->m_tm[tos][src][dst].allocatedRate;
                NS_LOG_INFO(" allocated rate is: " << m_storage->m_tm[tos][src][dst].allocatedRate);
                if (m_linkstat[m_storage->m_paths[tos][src][dst][l]].bandwidth < 0)
                  m_linkstat[m_storage->m_paths[tos][src][dst][l]].bandwidth = 0;
                NS_LOG_INFO("Updated link bandwidth for " << (int)m_storage->m_paths[tos][src][dst][l] << "-th link is: " << m_linkstat[m_storage->m_paths[tos][src][dst][l]].bandwidth);
             }
           }
       }
    }
  }*/

  // Rate allocation module: (Yuanxiang Gao adds)
  //First round allocation:
  //Find the global bottleneck link:
  int bottleneckLink;
  double tempUtil =-1.0;
  for (int l=0; l < (int)m_linkstat.size();++l) {
      if (m_linkstat[l].utilization > tempUtil) {
         tempUtil = m_linkstat[l].utilization;
         bottleneckLink = l;
      }
    //NS_LOG_INFO("Rate allocation module, bandwidth, of link " << l << " is: " << m_linkstat[l].bandwidth);
  }
  NS_LOG_INFO(" The global bottleneck is: " << (int)bottleneckLink << " with utilization: " << m_linkstat[bottleneckLink].utilization); 

  for (int tos = 0; tos < MAXTOS; ++tos)
    for (int src =0; src < m_topo->m_numHost; ++src) {
      for (int dst = 0; dst < m_topo->m_numHost; ++dst) { 
        if (m_storage->m_tm[tos][src][dst].flowCounts != 0 && m_storage->m_tm[tos][src][dst].ratedFlag == false)
	  { 
            //NS_LOG_INFO(" One unrated flow is, src: " << src << " dst: " << dst << " tos: " << (int)tos << " flowCounts*streamSize " << (int)m_storage->m_tm[tos][src][dst].flowCounts*m_storage->m_tm[tos][src][dst].streamSize);
            for (int i = 0; i < (int)m_storage->m_paths[tos][src][dst].size(); ++i) 

              //When one round allocation is enabled following judgement should be uncomment:
              //if (true)
	      if (m_storage->m_paths[tos][src][dst][i]==bottleneckLink) // after || is the switch of invoking the maxMinIter logic or not; true: close the maxMinIter; false: open the maxMinIter.
		{
		  //allocating rate to this flow: (coflow max-min fairness)
                  m_storage->m_tm[tos][src][dst].lastTimeRate = m_storage->m_tm[tos][src][dst].allocatedRate;
		  m_storage->m_tm[tos][src][dst].allocatedRate = 1/tempUtil*apps_Weight.find(tos)->second*m_storage->m_tm[tos][src][dst].idealRate;

                  //per-flow max-min fairness:
		  //m_storage->m_tm[tos][src][dst].allocatedRate = 1/tempUtil * apps_Weight.find(tos)->second * m_storage->m_tm[tos][src][dst].flowCounts;
 
		  m_storage->m_tm[tos][src][dst].ratedFlag = true;

                  //NS_LOG_INFO("It's bottlenecked link is: " << (int)m_storage->m_paths[tos][src][dst][i] << " allocated rate is: " << m_storage->m_tm[tos][src][dst].allocatedRate << " rated flag is: " << (int)m_storage->m_tm[tos][src][dst].ratedFlag);

		  for (int l = 0; l < (int)m_storage->m_paths[tos][src][dst].size(); ++l) {
		    m_linkstat[m_storage->m_paths[tos][src][dst][l]].bandwidth -= m_storage->m_tm[tos][src][dst].allocatedRate;
                    //NS_LOG_INFO("Updated link bandwidth for " << (int)m_storage->m_paths[tos][src][dst][l] << "-th link is: " << m_linkstat[m_storage->m_paths[tos][src][dst][l]].bandwidth);
                  }
		  break;
		}
          }
        }
      }
   //NS_LOG_INFO("I'm right at location 2!");
   
   //When one-round allocation is enabled, please comment following codes:
   //Max-min iterations:
   while (true) { 
   int numUnratedFlows = 0;
   for (int tos = 0; tos < MAXTOS; ++tos)
     for (int src =0; src < m_topo->m_numHost; ++src)
       for (int dst = 0; dst < m_topo->m_numHost; ++dst)
         if (m_storage->m_tm[tos][src][dst].flowCounts != 0 && m_storage->m_tm[tos][src][dst].ratedFlag == false)
           numUnratedFlows++;
   NS_LOG_INFO("numUnratedFlows: " << numUnratedFlows);
   if (numUnratedFlows!=0)
     maxMinIter();
   else break;
   }

  //Distribute the rate of each flow to end-host:
  for (uint32_t tos = 0; tos < (uint32_t)MAXTOS; ++tos) {
    for (int src = 0; src < m_topo->m_numHost; ++src) {
      for (int dst = 0; dst < m_topo->m_numHost; ++dst) {
         if (m_storage->m_tm[tos][src][dst].flowCounts!=0) {
           double demand = m_storage->m_tm[tos][src][dst].allocatedRate;
           // record demand info
           StreamDemandInfo demandInfo;
           demandInfo.src = src;
           Ipv4Address dst_Ipv4 = Ipv4Address(m_topo->GetIpv4(dst));
           demandInfo.dst = dst_Ipv4;
           demandInfo.tos = tos;
           demandInfo.demand = demand;

           //sends it to each host agent:
           EventData eventData;
           eventData.rateData = demandInfo;
           uint32_t host = m_topo->GetIpv4(src);
           if (m_storage->m_socketMap.find(host) != m_storage->m_socketMap.end()) {
             NS_LOG_INFO("ControllAlgorithm::reallocateRates: src: " << src  << "dst: " << dst_Ipv4  << "tos: " << tos << "rate: " << demandInfo.demand);
             eventData.socket = m_storage->m_socketMap[host];
             m_mb->AddEvent(ALGOTOHOST, eventData);
           }
           else {
             NS_LOG_WARN("No such socket!");
           }
         }
       }
     }
   }

  if (Simulator::Now().GetSeconds() + m_period <= m_simuTime)
  Simulator::Schedule(Seconds(m_period), &ControlAlgorithm::CalculateNewPaths, this); 

  return true;
}

void ControlAlgorithm::maxMinIter() {
   //Update ideal rates and link utilization:
   //bool flag = false; //Whether there is at least one unrated flow;
   for (int l = 0; l < (int)m_linkstat.size(); ++l)
   {
     m_linkstat[l].utilization = 0;
   }
   for (int tos = 0; tos < MAXTOS; ++tos){
    //bool flag = false; //whether the coflow has at least one unrated flow.
    std::vector<int> bottleneckCounts(m_linkstat.size(),0); 
    for (int src = 0; src < m_topo->m_numHost; ++src){
      for (int dst = 0; dst < m_topo->m_numHost; ++dst){
        //Yuanxiang Gao adds:
        if (m_storage->m_tm[tos][src][dst].flowCounts!=0 && m_storage->m_tm[tos][src][dst].ratedFlag == false)
	{
          //flag = true;
	  for (int l = 0; l < (int)m_storage->m_paths[tos][src][dst].size(); ++l)
      	  {  
             bottleneckCounts[m_storage->m_paths[tos][src][dst][l]] += m_storage->m_tm[tos][src][dst].flowCounts * m_storage->m_tm[tos][src][dst].streamSize;
             
             //coflow max-min fairness:
             m_linkstat[m_storage->m_paths[tos][src][dst][l]].utilization += m_storage->m_tm[tos][src][dst].idealRate*apps_Weight.find(tos)->second/m_linkstat[m_storage->m_paths[tos][src][dst][l]].bandwidth;
             
             //per-flow max-min fairness:
             //m_linkstat[m_storage->m_paths[tos][src][dst][l]].utilization += m_storage->m_tm[tos][src][dst].flowCounts*apps_Weight.find(tos)->second/m_linkstat[m_storage->m_paths[tos][src][dst][l]].bandwidth;
    
             //NS_LOG_INFO(" The utilization of link " << m_storage->m_paths[tos][src][dst][l] << " is: " << m_linkstat[m_storage->m_paths[tos][src][dst][l]].utilization);
          }
         }	
       }
    } 
    //for (int l=0; l<(int)bottleneckCounts.size(); ++l)
      //NS_LOG_INFO("The flow demands on link " << l << " is: " << bottleneckCounts[l] << " tos is: " << tos);
 }
 //Finding the new global bottleneck link:
  int bottleneckLink;
  double tempUtil =-1.0;
   for (int l = 0; l < (int)m_linkstat.size(); ++l)
   { 
     //NS_LOG_INFO("The " << l << "-th link has utilization of: " << m_linkstat[l].utilization);
   }
  for (int l=0; l < (int)m_linkstat.size();++l) {
      if (m_linkstat[l].utilization > tempUtil) {
         tempUtil = m_linkstat[l].utilization;
         bottleneckLink = l;
      }
  }
  NS_LOG_INFO(" The global bottleneck link is: " << (int)bottleneckLink << ", the maximal utilization is: " << tempUtil);
  for (int tos = 0; tos < MAXTOS; ++tos)
    for (int src =0; src < m_topo->m_numHost; ++src) {
      for (int dst = 0; dst < m_topo->m_numHost; ++dst) { 
        if (m_storage->m_tm[tos][src][dst].flowCounts != 0 && m_storage->m_tm[tos][src][dst].ratedFlag == false)
	  { 
            
            for (int i = 0; i < (int)m_storage->m_paths[tos][src][dst].size(); ++i) {
              //NS_LOG_INFO("tos: " << tos << " src: " << src << " dst: " << dst << " through link: " << (int)m_storage->m_paths[tos][src][dst][i]) ;
	      if ((int)m_storage->m_paths[tos][src][dst][i]==bottleneckLink)
		{
		  //NS_LOG_INFO("the bottleneck link is: " << (int)bottleneckLink << " of the flow with tos: " << tos << " src: " << src << " dst: " <<dst);
                  //allocating rate to the bottlenecked flow (coflow max-min fairness):
		  m_storage->m_tm[tos][src][dst].allocatedRate = 1/tempUtil*apps_Weight.find(tos)->second*m_storage->m_tm[tos][src][dst].idealRate;
    
                  // per-flow max-min:
                  //m_storage->m_tm[tos][src][dst].allocatedRate = 1/tempUtil*apps_Weight.find(tos)->second * m_storage->m_tm[tos][src][dst].flowCounts; 

		  m_storage->m_tm[tos][src][dst].ratedFlag = true;
                  NS_LOG_INFO("The allocated rate of the flow, tos: " << tos << " src: " << src << " dst: " << dst << " rate: " << m_storage->m_tm[tos][src][dst].allocatedRate);
		  for (int l = 0; l < (int)m_storage->m_paths[tos][src][dst].size(); ++l)
                  {
		    m_linkstat[m_storage->m_paths[tos][src][dst][l]].bandwidth -= m_storage->m_tm[tos][src][dst].allocatedRate;
 		    //NS_LOG_INFO(" The bandwidth of link: " << m_storage->m_paths[tos][src][dst][l] << " is: " << m_linkstat[m_storage->m_paths[tos][src][dst][l]].bandwidth);
                  }
		  break;
		}
            }
           }
      }
    }
}

//Yuanxiang modifies this function:
bool ControlAlgorithm::ReRouteAllDemands(std::vector<StreamDemandInfo> &demandVec) {

  NS_LOG_FUNCTION(this);

  std::vector<Path_t> newPaths;
  std::vector<StreamDemandInfo> successDemands;

  for (int j = 0; j < (int)demandVec.size(); ++j) {
    //NS_LOG_INFO("DEMANDVEC.src " << demandVec[j].src);
    //NS_LOG_INFO("DEMANDVEC.dst " << demandVec[j].dstHost);
    //NS_LOG_INFO("DEMANDVEC.tos " << demandVec[j].tos);
  }

  for (int j = 0; j < (int)demandVec.size(); ++j) {
    int tos = demandVec[j].tos;
    int src = demandVec[j].src;
    int dst = demandVec[j].dstHost;
    double demand = demandVec[j].demand;
    Path_t path = m_storage->m_paths[tos][src][dst];

    std::vector<Path_t> alterPaths = m_storage->m_allPaths[src][dst];

    uint8_t order=0;
    for(std::vector<Path_t>::iterator it1=alterPaths.begin();it1!=alterPaths.end();++it1) {
      order++;
      for(std::vector<int>::iterator it2=(*it1).begin();it2!=(*it1).end();++it2) {
        //NS_LOG_INFO("alternative paths: " << order);
        //NS_LOG_INFO("reroute demand module, src: " << src << " dst: " << dst << " tos: " << tos << " through link: " << *it2);
      }
    }
	         
    int size = alterPaths.size();

    int p=0;

    /*//Heuristic-based rerouting scheme:
    int k;
    double tempUtil;
    double tempTime;
    tempUtil = 100000;
    tempTime = 100000;

    for (k = 0; k < size; ++k) {
      double maxUtil = 0.0;
      double maxTime = 0.0;
      Path_t alterPath = alterPaths[k];
      for (std::vector<int>::iterator it=alterPath.begin();it!=alterPath.end();++it)
        NS_LOG_INFO(" candidate path, through link: " << *it);
      for (int l = 0; l < (int)alterPath.size(); l++) {
        NS_LOG_INFO(" For link " << alterPath[l] << " reroute module, current streamSize: " << demandVec[j].streamSize << " current totalTime: " << m_linkstat[alterPath[l]].totalTime << " current bandwidth: " << m_linkstat[l].bandwidth);
        if((m_linkstat[alterPath[l]].totalTime+demandVec[j].streamSize/m_linkstat[l].bandwidth)>maxTime) {
          maxTime = m_linkstat[alterPath[l]].totalTime+demandVec[j].streamSize/m_linkstat[l].bandwidth; 
        }
        if((m_linkstat[alterPath[l]].utilization+demandVec[j].demand/m_linkstat[l].bandwidth)>maxUtil) {
            maxUtil=m_linkstat[alterPath[l]].utilization+demandVec[j].demand/m_linkstat[l].bandwidth;
        }
      }
      NS_LOG_INFO(" reroute module, current max time along the path: " << maxTime);
      NS_LOG_INFO(" reroute module, current max utilization along the path: " << maxUtil);
      if (maxUtil<=tempUtil) {
        if (maxUtil == tempUtil) {
          if (maxTime < tempTime) {
          tempUtil = maxUtil;
          tempTime = maxTime;
          p = k;
          }
        }
        else {
           tempUtil = maxUtil;
           tempTime = maxTime;
           p = k;
        }
       }
    }*/

    //ECMP based random rerouting:
    //We enforce the ECMP find a path with exact 4 links:f
    do {
        p = rand()%size;
    }while (alterPaths[p].size()!=4 && alterPaths[p].size()!=2);
    
    NS_LOG_INFO(" finally chosen path: " << p);

      // Yuanxiang modifies:
      // find an alter path
      Path_t nowPath = alterPaths[p];
            
      NS_LOG_INFO("ReRoute: old path ");
      for (int idx = 0; idx < (int)path.size(); ++idx) {
        NS_LOG_INFO("ReRoute: " << path[idx]);
      }
  
      NS_LOG_INFO("ReRoute: new path ");
      for (int idx = 0; idx < (int)nowPath.size(); ++idx) {
        NS_LOG_INFO("ReRoute: " << nowPath[idx]);
      }
      
      // record alter path
      newPaths.push_back(nowPath);
      successDemands.push_back(demandVec[j]);
      
      ////update link status
      for (int l = 0; l < (int)nowPath.size(); ++l) {
        m_linkstat[nowPath[l]].utilization += ( demand / m_linkstat[nowPath[l]].bandwidth );
        m_linkstat[nowPath[l]].totalTime += ( demandVec[j].streamSize / m_linkstat[nowPath[l]].bandwidth );
      }
   }
    
  SetDynamicPaths(newPaths, successDemands);
  return true;
}

void ControlAlgorithm::SetDynamicPaths(std::vector<Path_t> newPaths,
                                       std::vector<StreamDemandInfo> demands) {
  NS_LOG_FUNCTION(this);
  
  NS_LOG_INFO("CONTROLALGORITHM: SetDefaultPaths " <<
              Simulator::Now().GetSeconds());
  
  for (int i = 0; i < (int)demands.size(); ++i) {
    NS_LOG_INFO("Am I right here?");
    int tos = demands[i].tos;
    int src = demands[i].src;
    int dst = demands[i].dstHost;
    //NS_LOG_INFO("ReRoute tos = " << tos);
    //NS_LOG_INFO("ReRoute src = " << src);
    //NS_LOG_INFO("ReRoute dst = " << dst);
    Path_t path = m_storage->m_paths[tos][src][dst];

    // modify or add path
    for (int j = 1; j < (int)newPaths[i].size(); ++j) {
      bool found = false;
      int in, out;
      int inPort = m_topo->m_edges[newPaths[i][j-1]].dpt;
      int outPort = m_topo->m_edges[newPaths[i][j]].spt;
      int sw = m_topo->m_edges[newPaths[i][j]].src;
      //NS_LOG_INFO("ReRoute inport = " << inPort << " outport = " << outPort);

      for (int k = 1; k < (int)path.size(); ++k) {
        if (m_topo->m_edges[newPaths[i][j]].src == m_topo->m_edges[path[k]].src) {
          found = true;
          in = m_topo->m_edges[path[k-1]].dpt;
          out = m_topo->m_edges[path[k]].spt;
          break;
        }
      }

      if (found) {
        // modify flow entry
        //NS_LOG_INFO("ReRoute modify a flow entry at switch " << sw);
        //NS_LOG_INFO("ReRoute prevIn = " << in << " prevOut = " << out);
        if (inPort == in) {
          ModFlow(OFPFC_MODIFY, sw, inPort, outPort, src, dst, tos);
        }
        else {
          ModFlow(OFPFC_ADD, sw, inPort, outPort, src, dst, tos);
          ModFlow(OFPFC_DELETE, sw, in, out, src, dst, tos);
        }
      }
      else {
        // add flow entry
        //NS_LOG_INFO("ReRoute add a flow entry at switch " << sw);
        ModFlow(OFPFC_ADD, sw, inPort, outPort, src, dst, tos);
      }
    }

    // remove path
    for (int j = 1; j < (int)path.size(); ++j) {
      bool found = false;
      int inPort = m_topo->m_edges[path[j-1]].dpt;
      int outPort = m_topo->m_edges[path[j]].spt;
      int sw = m_topo->m_edges[path[j]].src;
      NS_LOG_INFO("ReRoute inport = " << inPort << " outport = " << outPort);

      for (int k = 1; k < (int)newPaths[i].size(); ++k) {
        if (m_topo->m_edges[path[j]].src == m_topo->m_edges[newPaths[i][k]].src) {
          found = true;
          break;
        }
      }
      if (!found) {
        // remove flow entry
        //NS_LOG_INFO("ReRoute remove a flow entry at switch " << sw);
        ModFlow(OFPFC_DELETE, sw, inPort, outPort, src, dst, tos);
      }
    }

    m_storage->m_paths[tos][src][dst] = newPaths[i];  
  }
  NS_LOG_INFO("Am I right here?");
}


void ControlAlgorithm::ChangeWFQWeight() {
  NS_LOG_FUNCTION(this);

  std::map<WFQPort, std::vector<uint32_t> >::iterator iter;
  for (iter = m_storage->m_wfqPorts.begin(); iter != m_storage->m_wfqPorts.end(); ++iter) {
    for (int i = 0; i < (int)(iter->second.size()); ++i) {
      NS_LOG_INFO("which node: " << iter->first.nodeId << " which port: " << iter->first.portId << "Ports Weight " << iter->second[i]);
    }
    NS_LOG_INFO("Ports Weight *****************");
  }
  
  bool updateFlag = false;
  for (int i = 0; i < MAXTOS; ++i) {
    if(m_storage->m_currentWeight[i]!=m_storage->m_previousWeight[i]) {
      m_storage->m_previousWeight=m_storage->m_currentWeight;
      updateFlag=true;
      break;
      }
  }
 

  for (std::map<uint32_t, Ptr<Socket> >::iterator it=m_storage->m_socketMap.begin();it!=m_storage->m_socketMap.end();++it)
    NS_LOG_INFO(" Let's see which sockets we have, Ipv4: " << Ipv4Address(it->first) << " socket: " << it->second);  
        // change WFQ weight at every ports
    if (updateFlag==true) {
        for (iter = m_storage->m_wfqPorts.begin(); iter != m_storage->m_wfqPorts.end(); ++iter) {

          iter->second=m_storage->m_currentWeight;
          EventData eventData;
          eventData.port = iter->first;
          uint32_t host = m_topo->GetIpv4(iter->first.nodeId);

          if (m_storage->m_socketMap.find(host) != m_storage->m_socketMap.end()) 
          {
            NS_LOG_INFO("ChangeWFQWeight" << iter->first.nodeId << " " << m_topo->m_numHost);
            eventData.socket = m_storage->m_socketMap[host];

            if (iter->first.nodeId < m_topo->m_numHost) {
              m_mb->AddEvent(ALGOTOHOST, eventData);
            } else {
              m_mb->AddEvent(ALGOTOSWITCH, eventData);
            }
          } else {
            NS_LOG_WARN("No such socket");
          }
       }
    }
  

  if (Simulator::Now().GetSeconds() + m_period <= m_simuTime)
  Simulator::Schedule(Seconds(m_period), &ControlAlgorithm::ChangeWFQWeight, this);
}

void ControlAlgorithm::ModFlow (uint16_t command, uint32_t sw, uint16_t in_port, uint16_t out_port, uint32_t src, uint32_t dst, uint8_t tos)
{
  EventData data;
  data.flow.sw = m_topo->GetIpv4 (sw);
  data.flow.command = command;
  data.flow.in_port = in_port;
  data.flow.out_port = out_port;
  m_topo->CopyMac (src, data.flow.mac_src);
  m_topo->CopyMac (dst, data.flow.mac_dst);
  data.flow.ip_src = m_topo->GetIpv4 (src);
  data.flow.ip_dst = m_topo->GetIpv4 (dst);
  data.flow.ip_proto = 17;
  data.flow.tos = tos;

  m_mb->AddEvent (ALGOTOSWITCH, data);
}

} // namespace ns3
