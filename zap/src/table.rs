use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::{Arc, Mutex},
};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{internal_datafusion_err, project_schema},
    datasource::TableType,
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
    prelude::Expr,
};

pub struct StreamWrapper(pub Mutex<Option<SendableRecordBatchStream>>);

impl From<SendableRecordBatchStream> for StreamWrapper {
    fn from(stream: SendableRecordBatchStream) -> Self {
        Self(Mutex::new(Some(stream)))
    }
}

impl Debug for StreamWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("StreamWrapper").finish()
    }
}

#[derive(Debug)]
pub struct OneShotStreamProvider {
    pub schema: SchemaRef,
    pub stream: Arc<StreamWrapper>,
}

#[async_trait::async_trait]
impl TableProvider for OneShotStreamProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(OneShotStreamExec::try_new(
            self.schema.clone(),
            projection,
            self.stream.clone(),
        )?))
    }
}

#[derive(Debug)]
pub struct OneShotStreamExec {
    stream: Arc<StreamWrapper>,
    properties: PlanProperties,
}

impl OneShotStreamExec {
    pub fn try_new(
        schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        stream: Arc<StreamWrapper>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection)?;
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self { stream, properties })
    }
}

impl DisplayAs for OneShotStreamExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Exec")
    }
}

impl ExecutionPlan for OneShotStreamExec {
    fn name(&self) -> &str {
        "data"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.clone())
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self
            .stream
            .0
            .lock()
            .map_err(|err| internal_datafusion_err!("{}", err))?
            .take()
            .ok_or_else(|| internal_datafusion_err!("stream was already taken"))?;
        Ok(stream)
    }
}
