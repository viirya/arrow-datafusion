// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::common::DFSchema;
use datafusion::logical_expr::{FetchType, Limit, LogicalPlan, SkipType};
use std::sync::Arc;
use substrait::proto::rel::RelType;
use substrait::proto::{FetchRel, Rel, fetch_rel};

pub fn from_limit(
    producer: &mut impl SubstraitProducer,
    limit: &Limit,
) -> datafusion::common::Result<Box<Rel>> {
    // Pushing down LIMIT + OFFSET can produce a scan fetch above i64::MAX.
    // It is redundant when this Limit requests no rows beyond that bound.
    // Remove only that redundant fetch; retain the scan's exact offset.
    let mut input_plan = None;
    if let LogicalPlan::TableScan(scan) = limit.input.as_ref()
        && let Some(scan_fetch) = scan.fetch
        && i64::try_from(scan_fetch).is_err()
        && let FetchType::Literal(Some(fetch)) = limit.get_fetch_type()?
        && let SkipType::Literal(skip) = limit.get_skip_type()?
        && skip.checked_add(fetch).is_some_and(|end| end <= scan_fetch)
    {
        let mut scan = scan.clone();
        scan.fetch = None;
        input_plan = Some(LogicalPlan::TableScan(scan));
    }
    let input = producer.handle_plan(input_plan.as_ref().unwrap_or(&limit.input))?;
    let empty_schema = Arc::new(DFSchema::empty());

    let offset_mode = limit
        .skip
        .as_ref()
        .map(|expr| producer.handle_expr(expr.as_ref(), &empty_schema))
        .transpose()?
        .map(Box::new)
        .map(fetch_rel::OffsetMode::OffsetExpr);
    let count_mode = limit
        .fetch
        .as_ref()
        .map(|expr| producer.handle_expr(expr.as_ref(), &empty_schema))
        .transpose()?
        .map(Box::new)
        .map(fetch_rel::CountMode::CountExpr);
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Fetch(Box::new(FetchRel {
            common: None,
            input: Some(input),
            offset_mode,
            count_mode,
            advanced_extension: None,
        }))),
    }))
}
