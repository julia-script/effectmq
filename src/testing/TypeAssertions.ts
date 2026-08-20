/** Compile-time assertions used by public declaration contract tests. */

export type IsAny<A> = 0 extends 1 & A ? true : false;

export type IsUnknown<A> =
  IsAny<A> extends true
    ? false
    : unknown extends A
      ? [keyof A] extends [never]
        ? true
        : false
      : false;

export type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? (<T>() => T extends B ? 1 : 2) extends <T>() => T extends A ? 1 : 2
      ? true
      : false
    : false;

export type Expect<Condition extends true> = Condition;

export type ExpectFalse<Condition extends false> = Condition;
