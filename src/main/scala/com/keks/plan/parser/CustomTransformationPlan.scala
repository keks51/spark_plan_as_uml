package com.keks.plan.parser


case class CustomTransformationPlan(rootNode: TransformationPlanNode,
                                    nodesList: Array[TransformationPlanNode],
                                    edgesList: Array[(Int, Int)])
