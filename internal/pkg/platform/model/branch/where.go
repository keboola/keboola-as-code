// Code generated by ent, DO NOT EDIT.

package branch

import (
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/key"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/predicate"
)

// ID filters vertices based on their ID field.
func ID(id key.BranchKey) predicate.Branch {
	return predicate.Branch(sql.FieldEQ(FieldID, id))
}

// IDEQ applies the EQ predicate on the ID field.
func IDEQ(id key.BranchKey) predicate.Branch {
	return predicate.Branch(sql.FieldEQ(FieldID, id))
}

// IDNEQ applies the NEQ predicate on the ID field.
func IDNEQ(id key.BranchKey) predicate.Branch {
	return predicate.Branch(sql.FieldNEQ(FieldID, id))
}

// IDIn applies the In predicate on the ID field.
func IDIn(ids ...key.BranchKey) predicate.Branch {
	return predicate.Branch(sql.FieldIn(FieldID, ids...))
}

// IDNotIn applies the NotIn predicate on the ID field.
func IDNotIn(ids ...key.BranchKey) predicate.Branch {
	return predicate.Branch(sql.FieldNotIn(FieldID, ids...))
}

// IDGT applies the GT predicate on the ID field.
func IDGT(id key.BranchKey) predicate.Branch {
	return predicate.Branch(sql.FieldGT(FieldID, id))
}

// IDGTE applies the GTE predicate on the ID field.
func IDGTE(id key.BranchKey) predicate.Branch {
	return predicate.Branch(sql.FieldGTE(FieldID, id))
}

// IDLT applies the LT predicate on the ID field.
func IDLT(id key.BranchKey) predicate.Branch {
	return predicate.Branch(sql.FieldLT(FieldID, id))
}

// IDLTE applies the LTE predicate on the ID field.
func IDLTE(id key.BranchKey) predicate.Branch {
	return predicate.Branch(sql.FieldLTE(FieldID, id))
}

// BranchID applies equality check predicate on the "branchID" field. It's identical to BranchIDEQ.
func BranchID(v keboola.BranchID) predicate.Branch {
	vc := int(v)
	return predicate.Branch(sql.FieldEQ(FieldBranchID, vc))
}

// Name applies equality check predicate on the "name" field. It's identical to NameEQ.
func Name(v string) predicate.Branch {
	return predicate.Branch(sql.FieldEQ(FieldName, v))
}

// Description applies equality check predicate on the "description" field. It's identical to DescriptionEQ.
func Description(v string) predicate.Branch {
	return predicate.Branch(sql.FieldEQ(FieldDescription, v))
}

// IsDefault applies equality check predicate on the "isDefault" field. It's identical to IsDefaultEQ.
func IsDefault(v bool) predicate.Branch {
	return predicate.Branch(sql.FieldEQ(FieldIsDefault, v))
}

// BranchIDEQ applies the EQ predicate on the "branchID" field.
func BranchIDEQ(v keboola.BranchID) predicate.Branch {
	vc := int(v)
	return predicate.Branch(sql.FieldEQ(FieldBranchID, vc))
}

// BranchIDNEQ applies the NEQ predicate on the "branchID" field.
func BranchIDNEQ(v keboola.BranchID) predicate.Branch {
	vc := int(v)
	return predicate.Branch(sql.FieldNEQ(FieldBranchID, vc))
}

// BranchIDIn applies the In predicate on the "branchID" field.
func BranchIDIn(vs ...keboola.BranchID) predicate.Branch {
	v := make([]any, len(vs))
	for i := range v {
		v[i] = int(vs[i])
	}
	return predicate.Branch(sql.FieldIn(FieldBranchID, v...))
}

// BranchIDNotIn applies the NotIn predicate on the "branchID" field.
func BranchIDNotIn(vs ...keboola.BranchID) predicate.Branch {
	v := make([]any, len(vs))
	for i := range v {
		v[i] = int(vs[i])
	}
	return predicate.Branch(sql.FieldNotIn(FieldBranchID, v...))
}

// BranchIDGT applies the GT predicate on the "branchID" field.
func BranchIDGT(v keboola.BranchID) predicate.Branch {
	vc := int(v)
	return predicate.Branch(sql.FieldGT(FieldBranchID, vc))
}

// BranchIDGTE applies the GTE predicate on the "branchID" field.
func BranchIDGTE(v keboola.BranchID) predicate.Branch {
	vc := int(v)
	return predicate.Branch(sql.FieldGTE(FieldBranchID, vc))
}

// BranchIDLT applies the LT predicate on the "branchID" field.
func BranchIDLT(v keboola.BranchID) predicate.Branch {
	vc := int(v)
	return predicate.Branch(sql.FieldLT(FieldBranchID, vc))
}

// BranchIDLTE applies the LTE predicate on the "branchID" field.
func BranchIDLTE(v keboola.BranchID) predicate.Branch {
	vc := int(v)
	return predicate.Branch(sql.FieldLTE(FieldBranchID, vc))
}

// NameEQ applies the EQ predicate on the "name" field.
func NameEQ(v string) predicate.Branch {
	return predicate.Branch(sql.FieldEQ(FieldName, v))
}

// NameNEQ applies the NEQ predicate on the "name" field.
func NameNEQ(v string) predicate.Branch {
	return predicate.Branch(sql.FieldNEQ(FieldName, v))
}

// NameIn applies the In predicate on the "name" field.
func NameIn(vs ...string) predicate.Branch {
	return predicate.Branch(sql.FieldIn(FieldName, vs...))
}

// NameNotIn applies the NotIn predicate on the "name" field.
func NameNotIn(vs ...string) predicate.Branch {
	return predicate.Branch(sql.FieldNotIn(FieldName, vs...))
}

// NameGT applies the GT predicate on the "name" field.
func NameGT(v string) predicate.Branch {
	return predicate.Branch(sql.FieldGT(FieldName, v))
}

// NameGTE applies the GTE predicate on the "name" field.
func NameGTE(v string) predicate.Branch {
	return predicate.Branch(sql.FieldGTE(FieldName, v))
}

// NameLT applies the LT predicate on the "name" field.
func NameLT(v string) predicate.Branch {
	return predicate.Branch(sql.FieldLT(FieldName, v))
}

// NameLTE applies the LTE predicate on the "name" field.
func NameLTE(v string) predicate.Branch {
	return predicate.Branch(sql.FieldLTE(FieldName, v))
}

// NameContains applies the Contains predicate on the "name" field.
func NameContains(v string) predicate.Branch {
	return predicate.Branch(sql.FieldContains(FieldName, v))
}

// NameHasPrefix applies the HasPrefix predicate on the "name" field.
func NameHasPrefix(v string) predicate.Branch {
	return predicate.Branch(sql.FieldHasPrefix(FieldName, v))
}

// NameHasSuffix applies the HasSuffix predicate on the "name" field.
func NameHasSuffix(v string) predicate.Branch {
	return predicate.Branch(sql.FieldHasSuffix(FieldName, v))
}

// NameEqualFold applies the EqualFold predicate on the "name" field.
func NameEqualFold(v string) predicate.Branch {
	return predicate.Branch(sql.FieldEqualFold(FieldName, v))
}

// NameContainsFold applies the ContainsFold predicate on the "name" field.
func NameContainsFold(v string) predicate.Branch {
	return predicate.Branch(sql.FieldContainsFold(FieldName, v))
}

// DescriptionEQ applies the EQ predicate on the "description" field.
func DescriptionEQ(v string) predicate.Branch {
	return predicate.Branch(sql.FieldEQ(FieldDescription, v))
}

// DescriptionNEQ applies the NEQ predicate on the "description" field.
func DescriptionNEQ(v string) predicate.Branch {
	return predicate.Branch(sql.FieldNEQ(FieldDescription, v))
}

// DescriptionIn applies the In predicate on the "description" field.
func DescriptionIn(vs ...string) predicate.Branch {
	return predicate.Branch(sql.FieldIn(FieldDescription, vs...))
}

// DescriptionNotIn applies the NotIn predicate on the "description" field.
func DescriptionNotIn(vs ...string) predicate.Branch {
	return predicate.Branch(sql.FieldNotIn(FieldDescription, vs...))
}

// DescriptionGT applies the GT predicate on the "description" field.
func DescriptionGT(v string) predicate.Branch {
	return predicate.Branch(sql.FieldGT(FieldDescription, v))
}

// DescriptionGTE applies the GTE predicate on the "description" field.
func DescriptionGTE(v string) predicate.Branch {
	return predicate.Branch(sql.FieldGTE(FieldDescription, v))
}

// DescriptionLT applies the LT predicate on the "description" field.
func DescriptionLT(v string) predicate.Branch {
	return predicate.Branch(sql.FieldLT(FieldDescription, v))
}

// DescriptionLTE applies the LTE predicate on the "description" field.
func DescriptionLTE(v string) predicate.Branch {
	return predicate.Branch(sql.FieldLTE(FieldDescription, v))
}

// DescriptionContains applies the Contains predicate on the "description" field.
func DescriptionContains(v string) predicate.Branch {
	return predicate.Branch(sql.FieldContains(FieldDescription, v))
}

// DescriptionHasPrefix applies the HasPrefix predicate on the "description" field.
func DescriptionHasPrefix(v string) predicate.Branch {
	return predicate.Branch(sql.FieldHasPrefix(FieldDescription, v))
}

// DescriptionHasSuffix applies the HasSuffix predicate on the "description" field.
func DescriptionHasSuffix(v string) predicate.Branch {
	return predicate.Branch(sql.FieldHasSuffix(FieldDescription, v))
}

// DescriptionEqualFold applies the EqualFold predicate on the "description" field.
func DescriptionEqualFold(v string) predicate.Branch {
	return predicate.Branch(sql.FieldEqualFold(FieldDescription, v))
}

// DescriptionContainsFold applies the ContainsFold predicate on the "description" field.
func DescriptionContainsFold(v string) predicate.Branch {
	return predicate.Branch(sql.FieldContainsFold(FieldDescription, v))
}

// IsDefaultEQ applies the EQ predicate on the "isDefault" field.
func IsDefaultEQ(v bool) predicate.Branch {
	return predicate.Branch(sql.FieldEQ(FieldIsDefault, v))
}

// IsDefaultNEQ applies the NEQ predicate on the "isDefault" field.
func IsDefaultNEQ(v bool) predicate.Branch {
	return predicate.Branch(sql.FieldNEQ(FieldIsDefault, v))
}

// HasConfigurations applies the HasEdge predicate on the "configurations" edge.
func HasConfigurations() predicate.Branch {
	return predicate.Branch(func(s *sql.Selector) {
		step := sqlgraph.NewStep(
			sqlgraph.From(Table, FieldID),
			sqlgraph.Edge(sqlgraph.O2M, true, ConfigurationsTable, ConfigurationsColumn),
		)
		sqlgraph.HasNeighbors(s, step)
	})
}

// HasConfigurationsWith applies the HasEdge predicate on the "configurations" edge with a given conditions (other predicates).
func HasConfigurationsWith(preds ...predicate.Configuration) predicate.Branch {
	return predicate.Branch(func(s *sql.Selector) {
		step := sqlgraph.NewStep(
			sqlgraph.From(Table, FieldID),
			sqlgraph.To(ConfigurationsInverseTable, FieldID),
			sqlgraph.Edge(sqlgraph.O2M, true, ConfigurationsTable, ConfigurationsColumn),
		)
		sqlgraph.HasNeighborsWith(s, step, func(s *sql.Selector) {
			for _, p := range preds {
				p(s)
			}
		})
	})
}

// And groups predicates with the AND operator between them.
func And(predicates ...predicate.Branch) predicate.Branch {
	return predicate.Branch(func(s *sql.Selector) {
		s1 := s.Clone().SetP(nil)
		for _, p := range predicates {
			p(s1)
		}
		s.Where(s1.P())
	})
}

// Or groups predicates with the OR operator between them.
func Or(predicates ...predicate.Branch) predicate.Branch {
	return predicate.Branch(func(s *sql.Selector) {
		s1 := s.Clone().SetP(nil)
		for i, p := range predicates {
			if i > 0 {
				s1.Or()
			}
			p(s1)
		}
		s.Where(s1.P())
	})
}

// Not applies the not operator on the given predicate.
func Not(p predicate.Branch) predicate.Branch {
	return predicate.Branch(func(s *sql.Selector) {
		p(s.Not())
	})
}
