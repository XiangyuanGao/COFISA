/**
 * The default control algorithm for the project.
 * @data 2016-10
 */

#ifndef CONTROL_ALGORITHM_H
#define CONTROL_ALGORITHM_H

#include "ns3/common.h"
#include "ns3/storage.h"
#include "ns3/topology.h"
#include "ns3/simulator.h"
#include "ns3/message-bus.h"
#include "ns3/switch-msg-process.h"

namespace ns3 {

class ControlAlgorithm : public Object {

public:
  static TypeId GetTypeId(void);
  ControlAlgorithm();
  ControlAlgorithm(Ptr<Topology> topo);
  virtual ~ControlAlgorithm();

  void SetTopology(Ptr<Topology> topo);
  void SetTrafficMatrix(TrafficMatrixs tm);
  void SetStorage(Ptr<Storage> storage);
  void SetMessageBus(Ptr<MessageBus> mb);
  void SetSimuTime(double time);
  void SetPeriod(double period);
  void Start (void);

  // for path
  void SetDefaultPaths();
  void SetDynamicPaths(std::vector<Path_t>, std::vector<StreamDemandInfo>);

  //Yuanxiang adds:
  void DetermineIdealRate();
  void maxMinIter();

  void UpdateLinksUtilization();
  
  //// run this function periodically
  virtual bool CalculateNewPaths();
  bool ReRouteAllDemands(std::vector<StreamDemandInfo>&);

  // for WFQ
  virtual void ChangeWFQWeight();

  // wangxing added
  void ModFlow (uint16_t command, uint32_t sw, uint16_t in_port, uint16_t out_port, uint32_t src, uint32_t dst, uint8_t tos);

private:
  std::vector<LinkStatus> m_linkstat;
  Ptr<Topology> m_topo;
  Ptr<MessageBus> m_mb;
  Ptr<Storage> m_storage;
 
  double m_period;
  double m_simuTime;
  std::map<int, int> apps_Weight;
  std::multimap<double,int> apps_idealCCT;
};

} // namespace ns3

#endif  /* CONTROL_ALGORTTHM_H */
